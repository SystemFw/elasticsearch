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
import java.util.concurrent.atomic.AtomicReference;

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

    private record LocalState<T>(T state, boolean closed, boolean terminalUpdate, ActionListener<T> updateListener) {}

    private record LeaseState(Lease lease, long renewAtMillis, boolean renewalInProgress) {}

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

    private void update(ActiveTask handle, S newState, boolean terminal, ActionListener<S> listener) {
        final ActiveTask task = currentTask(handle);
        if (task == null) {
            listener.onFailure(new LeaseLostException("lease for task [" + handle.taskId() + "] is no longer active"));
            return;
        }
        try {
            queue.update(task.leaseState.lease(), newState, terminal, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private synchronized ActiveTask currentTask(ActiveTask handle) {
        final ActiveTask task = active.get(handle.taskId());
        return task == handle ? task : null;
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
                final LeaseState leaseState = task.leaseState;
                final long taskCheckAt = leaseState.renewalInProgress()
                    ? leaseState.lease().expiryMillis()
                    : Math.min(leaseState.renewAtMillis(), leaseState.lease().expiryMillis());
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
                final LeaseState leaseState = task.leaseState;
                if (nowMillis >= leaseState.lease().expiryMillis()) {
                    expired.add(task);
                } else if (leaseState.renewalInProgress() == false && nowMillis >= leaseState.renewAtMillis()) {
                    task.leaseState = new LeaseState(leaseState.lease(), leaseState.renewAtMillis(), true);
                    toRenew.add(task);
                }
            }
        }

        expired.forEach(
            task -> task.leaseLost(new LeaseLostException("lease for task [" + task.leaseState.lease().taskId() + "] has expired"))
        );
        toRenew.forEach(this::renew);
        scheduleNextLeaseCheck();
    }

    private void renew(ActiveTask task) {
        final Lease lease;
        synchronized (this) {
            if (running == false || active.get(task.taskId()) != task) {
                return;
            }
            lease = task.leaseState.lease();
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
            final LeaseState leaseState = task.leaseState;
            final long nowMillis = threadPool.absoluteTimeInMillis();
            if (leaseState.lease() != previousLease
                || renewedLease == null
                || sameLeaseIncarnation(previousLease, renewedLease) == false
                || renewedLease.expiryMillis() <= nowMillis) {
                task.leaseState = new LeaseState(leaseState.lease(), leaseState.renewAtMillis(), false);
                invalidLease = new LeaseLostException("queue returned an invalid renewed lease for task [" + task.taskId() + "]");
            } else {
                task.leaseState = new LeaseState(renewedLease, renewalTime(nowMillis, renewedLease.expiryMillis()), false);
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
            final LeaseState leaseState = task.leaseState;
            final long nowMillis = threadPool.absoluteTimeInMillis();
            if (failure instanceof LeaseLostException) {
                loseLease = true;
                task.leaseState = new LeaseState(leaseState.lease(), leaseState.lease().expiryMillis(), false);
            } else if (nowMillis >= leaseState.lease().expiryMillis()) {
                loseLease = true;
                task.leaseState = new LeaseState(leaseState.lease(), leaseState.renewAtMillis(), false);
                failure = new LeaseLostException("lease for task [" + task.taskId() + "] has expired", failure);
            } else {
                loseLease = false;
                final long normalRetryMillis = Math.max(1L, leaseDuration.millis() / 10L);
                final long remainingMillis = leaseState.lease().expiryMillis() - nowMillis;
                final long renewAtMillis = nowMillis + Math.clamp(remainingMillis / 2L, 1L, normalRetryMillis);
                task.leaseState = new LeaseState(leaseState.lease(), renewAtMillis, false);
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
            ActionListener.runAfter(
                ActionListener.wrap(
                    ignored -> {},
                    failure -> logger.debug(() -> "failed to release lease for task [" + task.taskId() + "]", failure)
                ),
                () -> taskEnded(task)
            )
        );
        try {
            queue.release(task.leaseState.lease(), listener);
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

        private final String taskId;

        // Replaced while holding TaskProcessorRuntime.this and read by queue operations after an authority check.
        private volatile LeaseState leaseState;

        private final AtomicReference<LocalState<S>> localState;

        private ActiveTask(S state, Lease lease, long renewAtMillis) {
            this.taskId = lease.taskId();
            this.leaseState = new LeaseState(lease, renewAtMillis, false);
            this.localState = new AtomicReference<>(new LocalState<>(state, false, false, null));
        }

        private String taskId() {
            return taskId;
        }

        private boolean canProcess() {
            return localState.get().closed() == false;
        }

        @Override
        public S state() {
            return localState.get().state();
        }

        @Override
        public void update(S newState, boolean terminal, ActionListener<S> listener) {
            Objects.requireNonNull(newState);
            Objects.requireNonNull(listener);

            final LocalState<S> current = localState.get();
            if (current.closed()) {
                listener.onFailure(new LeaseLostException("lease for task [" + taskId + "] is no longer active"));
                return;
            }
            if (current.updateListener() != null) {
                listener.onFailure(new IllegalStateException("task [" + taskId + "] already has a persistent state change in progress"));
                return;
            }

            final LocalState<S> pendingUpdate = new LocalState<>(current.state(), false, terminal, listener);
            final LocalState<S> witness = localState.compareAndExchange(current, pendingUpdate);
            if (witness != current) {
                final Exception failure = witness.closed()
                    ? new LeaseLostException("lease for task [" + taskId + "] is no longer active")
                    : new IllegalStateException("task [" + taskId + "] changed concurrently while starting a persistent state change");
                listener.onFailure(failure);
                return;
            }

            final ActionListener<S> operationListener = ActionListener.assertOnce(
                ActionListener.wrap(
                    persistedState -> stateChangeCompleted(pendingUpdate, persistedState),
                    failure -> stateChangeFailed(pendingUpdate, failure)
                )
            );
            TaskProcessorRuntime.this.update(this, newState, terminal, operationListener);
        }

        private void stateChangeCompleted(LocalState<S> pendingUpdate, S persistedState) {
            final LocalState<S> completed = new LocalState<>(
                Objects.requireNonNull(persistedState),
                pendingUpdate.terminalUpdate(),
                false,
                null
            );
            if (localState.compareAndSet(pendingUpdate, completed) == false) {
                return;
            }

            if (completed.closed()) {
                taskEnded(this);
            }
            pendingUpdate.updateListener().onResponse(persistedState);
        }

        private void stateChangeFailed(LocalState<S> pendingUpdate, Exception failure) {
            if (failure instanceof LeaseLostException) {
                leaseLost(failure);
                return;
            }

            final LocalState<S> failed = new LocalState<>(pendingUpdate.state(), false, false, null);
            if (localState.compareAndSet(pendingUpdate, failed)) {
                pendingUpdate.updateListener().onFailure(failure);
            }
        }

        private void processorFailed(Exception failure) {
            logger.warn(() -> "task processor failed unexpectedly for task [" + taskId + "]", failure);
            leaseLost(new LeaseLostException("task processor failed for task [" + taskId + "]", failure));
        }

        private boolean runtimeStopped() {
            final LocalState<S> previous = localState.getAndUpdate(
                current -> current.closed() ? current : new LocalState<>(current.state(), true, false, null)
            );
            if (previous.closed()) {
                return false;
            }

            final var failure = new LeaseLostException("runtime stopped while processing task [" + taskId + "]");
            cancelTask(this);
            if (previous.updateListener() != null) {
                notifyStateFailure(previous.updateListener(), failure);
            }
            return true;
        }

        private void leaseLost(Exception failure) {
            final LocalState<S> previous = localState.getAndUpdate(
                current -> current.closed() ? current : new LocalState<>(current.state(), true, false, null)
            );
            if (previous.closed()) {
                return;
            }

            cancelTask(this);
            taskEnded(this);
            if (previous.updateListener() != null) {
                notifyStateFailure(previous.updateListener(), failure);
            }
        }

        private void notifyStateFailure(ActionListener<S> listener, Exception failure) {
            try {
                listener.onFailure(failure);
            } catch (Exception e) {
                logger.warn(() -> "state-change listener failed while handling lease loss for task [" + taskId + "]", e);
            }
        }

    }
}
