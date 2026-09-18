/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.restore;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.snapshots.restore.Task.TaskHandle;
import org.elasticsearch.xpack.stateless.snapshots.restore.TaskQueue.Lease;
import org.elasticsearch.xpack.stateless.snapshots.restore.TaskQueue.LeaseLostException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * Runs a processor against tasks claimed from a {@link TaskQueue}, with bounded concurrency and centralized lease management.
 */
public final class TaskProcessorRuntime<S> extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(TaskProcessorRuntime.class);

    private final TaskQueue<S> queue;
    private final Task<S> processor;
    private final ThreadPool threadPool;
    private final Executor processorExecutor;
    private final String workerId;
    private final int maxConcurrentTasks;
    private final TimeValue leaseDuration;
    private final TimeValue claimInterval;
    private final Map<String, ActiveTask> active = new HashMap<>();

    // The following fields are guarded by this.
    private boolean running;
    private boolean claimInProgress;
    private Scheduler.Cancellable nextClaim;
    private Scheduler.Cancellable nextLeaseCheck;
    private long nextLeaseCheckAtMillis = Long.MAX_VALUE;
    private long leaseCheckGeneration;

    /** Creates a runtime for one task type and processor. */
    public TaskProcessorRuntime(
        TaskQueue<S> queue,
        Task<S> processor,
        ThreadPool threadPool,
        Executor processorExecutor,
        String workerId,
        int maxConcurrentTasks,
        TimeValue leaseDuration,
        TimeValue claimInterval
    ) {
        this.queue = Objects.requireNonNull(queue);
        this.processor = Objects.requireNonNull(processor);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.processorExecutor = Objects.requireNonNull(processorExecutor);
        this.workerId = Objects.requireNonNull(workerId);
        Objects.requireNonNull(leaseDuration);
        Objects.requireNonNull(claimInterval);
        if (maxConcurrentTasks <= 0) {
            throw new IllegalArgumentException("maximum concurrent tasks must be positive");
        }
        if (leaseDuration.millis() <= 1L) {
            throw new IllegalArgumentException("lease duration must be greater than one millisecond");
        }
        if (claimInterval.millis() <= 0L) {
            throw new IllegalArgumentException("claim interval must be positive");
        }
        this.maxConcurrentTasks = maxConcurrentTasks;
        this.leaseDuration = leaseDuration;
        this.claimInterval = claimInterval;
    }

    @Override
    protected void doStart() {
        synchronized (this) {
            running = true;
        }
        fillCapacity();
    }

    @Override
    protected void doStop() {
        final List<ActiveTask> tasks;
        synchronized (this) {
            running = false;
            cancelNextClaim();
            cancelNextLeaseCheck();
            tasks = List.copyOf(active.values());
        }
        for (ActiveTask task : tasks) {
            if (task.runtimeStopped()) {
                release(task);
            }
        }
    }

    @Override
    protected void doClose() {}

    private void fillCapacity() {
        final int capacity;
        synchronized (this) {
            if (running == false || claimInProgress) {
                return;
            }
            capacity = maxConcurrentTasks - active.size();
            if (capacity <= 0) {
                return;
            }
            claimInProgress = true;
        }

        final ActionListener<List<Tuple<S, Lease>>> listener = ActionListener.assertOnce(
            ActionListener.wrap(this::claimsCompleted, this::claimFailed)
        );
        try {
            queue.claim(workerId, capacity, leaseDuration, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void claimsCompleted(List<Tuple<S, Lease>> claimedTasks) {
        Objects.requireNonNull(claimedTasks);
        final List<ActiveTask> toStart = new ArrayList<>();
        final List<Lease> toRelease = new ArrayList<>();
        final boolean shouldPollLater;

        synchronized (this) {
            claimInProgress = false;
            if (running == false) {
                claimedTasks.forEach(task -> toRelease.add(task.v2()));
            } else {
                int remainingCapacity = maxConcurrentTasks - active.size();
                final long nowMillis = threadPool.absoluteTimeInMillis();
                for (Tuple<S, Lease> task : claimedTasks) {
                    final Lease lease = task.v2();
                    if (remainingCapacity > 0 && active.containsKey(lease.taskId()) == false && lease.expiryMillis() > nowMillis) {
                        var activeTask = new ActiveTask(task.v1(), lease, renewalTime(nowMillis, lease.expiryMillis()));
                        active.put(lease.taskId(), activeTask);
                        toStart.add(activeTask);
                        remainingCapacity--;
                    } else {
                        toRelease.add(lease);
                    }
                }
            }
            shouldPollLater = running && toStart.isEmpty();
        }

        toRelease.forEach(this::releaseUnstartedLease);
        scheduleNextLeaseCheck();
        toStart.forEach(this::startExecution);

        if (shouldPollLater) {
            scheduleNextClaim();
        } else {
            fillCapacity();
        }
    }

    private void claimFailed(Exception failure) {
        synchronized (this) {
            claimInProgress = false;
            if (running == false) {
                return;
            }
        }
        logger.debug("failed to claim queued tasks", failure);
        scheduleNextClaim();
    }

    private void scheduleNextClaim() {
        synchronized (this) {
            if (running == false || nextClaim != null || claimInProgress || active.size() >= maxConcurrentTasks) {
                return;
            }
            try {
                nextClaim = threadPool.schedule(() -> {
                    synchronized (TaskProcessorRuntime.this) {
                        nextClaim = null;
                    }
                    fillCapacity();
                }, claimInterval, threadPool.generic());
            } catch (Exception e) {
                logger.debug("failed to schedule the next task claim", e);
            }
        }
    }

    private void cancelNextClaim() {
        if (nextClaim != null) {
            nextClaim.cancel();
            nextClaim = null;
        }
    }

    private void startExecution(ActiveTask task) {
        if (task.canProcess() == false) {
            return;
        }
        try {
            processorExecutor.execute(() -> {
                if (task.canProcess() == false) {
                    return;
                }
                try {
                    processor.process(task);
                } catch (Exception e) {
                    task.processorFailed(e);
                }
            });
        } catch (Exception e) {
            task.processorFailed(e);
        }
    }

    private void cancelTask(ActiveTask task) {
        try {
            processor.cancel(task);
        } catch (Exception e) {
            logger.warn(() -> "task processor failed to cancel task [" + task.taskId() + "]", e);
        }
    }

    private void modify(ActiveTask handle, S newState, ActionListener<S> listener) {
        final ActiveTask task = currentTask(handle);
        if (task == null) {
            listener.onFailure(new LeaseLostException("lease for task [" + handle.taskId() + "] is no longer active"));
            return;
        }
        try {
            queue.modify(task.lease, newState, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void finish(ActiveTask handle, S finalState, ActionListener<S> listener) {
        final ActiveTask task = currentTask(handle);
        if (task == null) {
            listener.onFailure(new LeaseLostException("lease for task [" + handle.taskId() + "] is no longer active"));
            return;
        }
        final ActionListener<S> finishListener = ActionListener.wrap(listener::onResponse, e -> listener.onFailure(finishFailure(task, e)));
        try {
            queue.finish(task.lease, finalState, finishListener);
        } catch (Exception e) {
            finishListener.onFailure(e);
        }
    }

    private synchronized ActiveTask currentTask(ActiveTask handle) {
        final ActiveTask task = active.get(handle.taskId());
        return task == handle ? task : null;
    }

    private synchronized Exception finishFailure(ActiveTask task, Exception failure) {
        return task.deferredLeaseLoss == null ? failure : task.deferredLeaseLoss;
    }

    private void scheduleNextLeaseCheck() {
        List<ActiveTask> failedTasks = null;
        Exception schedulingFailure = null;
        synchronized (this) {
            if (running == false || active.isEmpty()) {
                cancelNextLeaseCheck();
                return;
            }

            long checkAtMillis = Long.MAX_VALUE;
            for (ActiveTask task : active.values()) {
                final long taskCheckAt = task.renewalInProgress
                    ? task.lease.expiryMillis()
                    : Math.min(task.renewAtMillis, task.lease.expiryMillis());
                checkAtMillis = Math.min(checkAtMillis, taskCheckAt);
            }
            if (nextLeaseCheck != null && nextLeaseCheckAtMillis <= checkAtMillis) {
                return;
            }

            cancelNextLeaseCheck();
            final long generation = ++leaseCheckGeneration;
            final long delayMillis = Math.max(1L, checkAtMillis - threadPool.absoluteTimeInMillis());
            try {
                nextLeaseCheckAtMillis = checkAtMillis;
                nextLeaseCheck = threadPool.schedule(
                    () -> checkLeases(generation),
                    TimeValue.timeValueMillis(delayMillis),
                    threadPool.generic()
                );
            } catch (Exception e) {
                nextLeaseCheck = null;
                nextLeaseCheckAtMillis = Long.MAX_VALUE;
                failedTasks = List.copyOf(active.values());
                schedulingFailure = e;
            }
        }

        if (failedTasks != null) {
            final var failure = new LeaseLostException("could not schedule lease management", schedulingFailure);
            failedTasks.forEach(task -> task.leaseLost(failure));
        }
    }

    private void checkLeases(long generation) {
        final List<ActiveTask> expired = new ArrayList<>();
        final List<ActiveTask> toRenew = new ArrayList<>();
        synchronized (this) {
            if (generation != leaseCheckGeneration) {
                return;
            }
            nextLeaseCheck = null;
            nextLeaseCheckAtMillis = Long.MAX_VALUE;
            if (running == false) {
                return;
            }

            final long nowMillis = threadPool.absoluteTimeInMillis();
            for (ActiveTask task : active.values()) {
                if (nowMillis >= task.lease.expiryMillis()) {
                    expired.add(task);
                } else if (task.renewalInProgress == false && nowMillis >= task.renewAtMillis) {
                    task.renewalInProgress = true;
                    toRenew.add(task);
                }
            }
        }

        expired.forEach(task -> task.leaseLost(new LeaseLostException("lease for task [" + task.lease.taskId() + "] has expired")));
        toRenew.forEach(this::renew);
        scheduleNextLeaseCheck();
    }

    private void renew(ActiveTask task) {
        final Lease lease;
        synchronized (this) {
            if (running == false || active.get(task.taskId()) != task) {
                return;
            }
            lease = task.lease;
        }
        final ActionListener<Lease> listener = ActionListener.assertOnce(
            ActionListener.wrap(renewedLease -> leaseRenewed(task, lease, renewedLease), e -> leaseRenewalFailed(task, e))
        );
        try {
            queue.renew(lease, leaseDuration, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void leaseRenewed(ActiveTask task, Lease previousLease, Lease renewedLease) {
        Exception invalidLease = null;
        synchronized (this) {
            if (running == false || active.get(task.taskId()) != task) {
                return;
            }
            task.renewalInProgress = false;
            final long nowMillis = threadPool.absoluteTimeInMillis();
            if (task.lease != previousLease
                || renewedLease == null
                || sameLeaseIncarnation(previousLease, renewedLease) == false
                || renewedLease.expiryMillis() <= nowMillis) {
                invalidLease = new LeaseLostException("queue returned an invalid renewed lease for task [" + task.taskId() + "]");
            } else {
                task.lease = renewedLease;
                task.renewAtMillis = renewalTime(nowMillis, renewedLease.expiryMillis());
            }
        }

        if (invalidLease == null) {
            scheduleNextLeaseCheck();
        } else {
            task.leaseLost(invalidLease);
        }
    }

    private void leaseRenewalFailed(ActiveTask task, Exception failure) {
        final boolean loseLease;
        synchronized (this) {
            if (running == false || active.get(task.taskId()) != task) {
                return;
            }
            task.renewalInProgress = false;
            final long nowMillis = threadPool.absoluteTimeInMillis();
            if (failure instanceof LeaseLostException) {
                loseLease = task.isFinishing() == false;
                task.renewAtMillis = task.lease.expiryMillis();
                if (loseLease == false) {
                    task.deferredLeaseLoss = failure;
                }
            } else if (nowMillis >= task.lease.expiryMillis()) {
                loseLease = true;
                failure = new LeaseLostException("lease for task [" + task.taskId() + "] has expired", failure);
            } else {
                loseLease = false;
                final long normalRetryMillis = Math.max(1L, leaseDuration.millis() / 10L);
                final long remainingMillis = task.lease.expiryMillis() - nowMillis;
                task.renewAtMillis = nowMillis + Math.clamp(remainingMillis / 2L, 1L, normalRetryMillis);
            }
        }

        if (loseLease) {
            task.leaseLost(failure);
        } else {
            scheduleNextLeaseCheck();
        }
    }

    private static boolean sameLeaseIncarnation(Lease current, Lease renewed) {
        return current.taskId().equals(renewed.taskId())
            && current.ownerId().equals(renewed.ownerId())
            && current.fencingToken() == renewed.fencingToken();
    }

    private static long renewalTime(long nowMillis, long expiryMillis) {
        return nowMillis + Math.max(1L, (expiryMillis - nowMillis) / 2L);
    }

    private void cancelNextLeaseCheck() {
        leaseCheckGeneration++;
        if (nextLeaseCheck != null) {
            nextLeaseCheck.cancel();
            nextLeaseCheck = null;
        }
        nextLeaseCheckAtMillis = Long.MAX_VALUE;
    }

    private void release(ActiveTask task) {
        final ActionListener<Void> listener = ActionListener.assertOnce(
            ActionListener.wrap(ignored -> task.releaseCompleted(null), task::releaseCompleted)
        );
        try {
            queue.release(task.lease, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void taskEnded(ActiveTask task) {
        final boolean refill;
        synchronized (this) {
            if (active.get(task.taskId()) == task) {
                active.remove(task.taskId());
            }
            refill = running;
        }
        scheduleNextLeaseCheck();
        if (refill) {
            fillCapacity();
        }
    }

    private void releaseUnstartedLease(Lease lease) {
        final ActionListener<Void> listener = ActionListener.wrap(
            ignored -> {},
            e -> logger.debug(() -> "failed to release unstarted task lease [" + lease.taskId() + "]", e)
        );
        try {
            queue.release(lease, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    // Exposed for tests.
    synchronized int activeTaskCount() {
        return active.size();
    }

    /** Holds the runtime and processor-facing state for one lease incarnation. */
    private final class ActiveTask implements TaskHandle<S> {

        private enum ChangeType {
            MODIFY,
            FINISH
        }

        private final String taskId;

        // Updated while holding TaskProcessorRuntime.this and read by queue operations after an authority check.
        private volatile Lease lease;

        // The following fields are guarded by TaskProcessorRuntime.this.
        private long renewAtMillis;
        private boolean renewalInProgress;
        private Exception deferredLeaseLoss;

        // The following fields are guarded by this.
        private S state;
        private boolean closed;
        private PendingChange pendingChange;

        private ActiveTask(S state, Lease lease, long renewAtMillis) {
            this.taskId = lease.taskId();
            this.state = state;
            this.lease = lease;
            this.renewAtMillis = renewAtMillis;
        }

        private String taskId() {
            return taskId;
        }

        private synchronized boolean canProcess() {
            return closed == false;
        }

        private synchronized boolean isFinishing() {
            return pendingChange != null && pendingChange.type == ChangeType.FINISH;
        }

        @Override
        public synchronized S state() {
            return state;
        }

        @Override
        public void modify(S newState, ActionListener<S> listener) {
            changeState(newState, ChangeType.MODIFY, listener);
        }

        @Override
        public void finish(S finalState, ActionListener<S> listener) {
            changeState(finalState, ChangeType.FINISH, listener);
        }

        private void changeState(S newState, ChangeType type, ActionListener<S> listener) {
            Objects.requireNonNull(newState);
            Objects.requireNonNull(listener);

            final Exception rejection;
            final PendingChange change;
            synchronized (this) {
                if (closed) {
                    change = null;
                    rejection = new LeaseLostException("lease for task [" + taskId + "] is no longer active");
                } else if (pendingChange != null) {
                    change = null;
                    rejection = new IllegalStateException("task [" + taskId + "] already has a persistent state change in progress");
                } else {
                    change = new PendingChange(type, listener);
                    pendingChange = change;
                    rejection = null;
                }
            }

            if (rejection != null) {
                listener.onFailure(rejection);
                return;
            }

            final ActionListener<S> operationListener = ActionListener.assertOnce(
                ActionListener.wrap(persistedState -> stateChangeCompleted(change, persistedState), e -> stateChangeFailed(change, e))
            );
            if (type == ChangeType.MODIFY) {
                TaskProcessorRuntime.this.modify(this, newState, operationListener);
            } else {
                TaskProcessorRuntime.this.finish(this, newState, operationListener);
            }
        }

        private void stateChangeCompleted(PendingChange change, S persistedState) {
            final boolean finished;
            synchronized (this) {
                if (pendingChange != change) {
                    return;
                }

                state = Objects.requireNonNull(persistedState);
                pendingChange = null;
                finished = change.type == ChangeType.FINISH;
                closed = finished;
            }

            if (finished) {
                taskEnded(this);
            }
            change.listener.onResponse(persistedState);
        }

        private void stateChangeFailed(PendingChange change, Exception failure) {
            if (failure instanceof LeaseLostException) {
                leaseLost(failure);
                return;
            }

            synchronized (this) {
                if (pendingChange != change) {
                    return;
                }
                pendingChange = null;
            }
            change.listener.onFailure(failure);
        }

        private void processorFailed(Exception failure) {
            logger.warn(() -> "task processor failed unexpectedly for task [" + taskId + "]", failure);
            leaseLost(new LeaseLostException("task processor failed for task [" + taskId + "]", failure));
        }

        private boolean runtimeStopped() {
            final PendingChange change;
            synchronized (this) {
                if (closed) {
                    return false;
                }
                closed = true;
                change = pendingChange;
                pendingChange = null;
            }

            final var failure = new LeaseLostException("runtime stopped while processing task [" + taskId + "]");
            cancelTask(this);
            if (change != null) {
                notifyStateFailure(change.listener, failure);
            }
            return true;
        }

        private void releaseCompleted(Exception failure) {
            if (failure != null) {
                logger.debug(() -> "failed to release lease for task [" + taskId + "]", failure);
            }
            taskEnded(this);
        }

        private void leaseLost(Exception failure) {
            final PendingChange change;
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                change = pendingChange;
                pendingChange = null;
            }

            cancelTask(this);
            taskEnded(this);
            if (change != null) {
                notifyStateFailure(change.listener, failure);
            }
        }

        private void notifyStateFailure(ActionListener<S> listener, Exception failure) {
            try {
                listener.onFailure(failure);
            } catch (Exception e) {
                logger.warn(() -> "state-change listener failed while handling lease loss for task [" + taskId + "]", e);
            }
        }

        private final class PendingChange {
            private final ChangeType type;
            private final ActionListener<S> listener;

            private PendingChange(ChangeType type, ActionListener<S> listener) {
                this.type = type;
                this.listener = listener;
            }
        }
    }
}
