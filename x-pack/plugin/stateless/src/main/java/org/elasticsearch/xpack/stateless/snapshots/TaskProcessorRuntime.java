/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.snapshots.TaskQueue.Lease;
import org.elasticsearch.xpack.stateless.snapshots.TaskQueue.LeaseLostException;
import org.elasticsearch.xpack.stateless.snapshots.TaskQueue.LeasedTask;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * Runs a processor against tasks claimed from a {@link TaskQueue}, with bounded concurrency and centralized lease management.
 */
public final class TaskProcessorRuntime<S> extends AbstractLifecycleComponent implements TaskImpl.Operations<S> {

    private static final Logger logger = LogManager.getLogger(TaskProcessorRuntime.class);

    private final TaskQueue<S> queue;
    private final Task<S> processor;
    private final ThreadPool threadPool;
    private final Executor processorExecutor;
    private final String workerId;
    private final int maxConcurrentTasks;
    private final TimeValue leaseDuration;
    private final TimeValue claimInterval;
    private final Map<String, ActiveTask<S>> active = new HashMap<>();

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
        final List<ActiveTask<S>> tasks;
        synchronized (this) {
            running = false;
            cancelNextClaim();
            cancelNextLeaseCheck();
            tasks = List.copyOf(active.values());
        }
        for (ActiveTask<S> task : tasks) {
            if (task.execution.runtimeStopped()) {
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

        final ActionListener<List<LeasedTask<S>>> listener = ActionListener.assertOnce(
            ActionListener.wrap(this::claimsCompleted, this::claimFailed)
        );
        try {
            queue.claim(workerId, capacity, leaseDuration, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void claimsCompleted(List<LeasedTask<S>> claimedTasks) {
        Objects.requireNonNull(claimedTasks);
        final List<TaskImpl<S>> toStart = new ArrayList<>();
        final List<Lease> toRelease = new ArrayList<>();
        final boolean shouldPollLater;

        synchronized (this) {
            claimInProgress = false;
            if (running == false) {
                claimedTasks.forEach(task -> toRelease.add(task.lease()));
            } else {
                int remainingCapacity = maxConcurrentTasks - active.size();
                final long nowMillis = threadPool.absoluteTimeInMillis();
                for (LeasedTask<S> task : claimedTasks) {
                    if (remainingCapacity > 0
                        && active.containsKey(task.lease().taskId()) == false
                        && task.lease().expiryMillis() > nowMillis) {
                        var execution = new TaskImpl<>(task.lease().taskId(), task.state(), this, this::cancelTask, this::executionEnded);
                        var activeTask = new ActiveTask<>(execution, task.lease(), renewalTime(nowMillis, task.lease().expiryMillis()));
                        active.put(task.lease().taskId(), activeTask);
                        toStart.add(execution);
                        remainingCapacity--;
                    } else {
                        toRelease.add(task.lease());
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

    private void startExecution(TaskImpl<S> execution) {
        if (execution.canProcess() == false) {
            return;
        }
        try {
            processorExecutor.execute(() -> {
                if (execution.canProcess() == false) {
                    return;
                }
                try {
                    processor.process(execution);
                } catch (Exception e) {
                    execution.processorFailed(e);
                }
            });
        } catch (Exception e) {
            execution.processorFailed(e);
        }
    }

    private void cancelTask(TaskImpl<S> task) {
        try {
            processor.cancel(task);
        } catch (Exception e) {
            logger.warn(() -> "task processor failed to cancel task [" + task.taskId() + "]", e);
        }
    }

    @Override
    public void modify(TaskImpl<S> execution, S newState, ActionListener<S> listener) {
        final ActiveTask<S> task = currentTask(execution);
        if (task == null) {
            listener.onFailure(new LeaseLostException("lease for task [" + execution.taskId() + "] is no longer active"));
            return;
        }
        try {
            queue.modify(task.lease, newState, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void finish(TaskImpl<S> execution, S finalState, ActionListener<S> listener) {
        final ActiveTask<S> task = currentTask(execution);
        if (task == null) {
            listener.onFailure(new LeaseLostException("lease for task [" + execution.taskId() + "] is no longer active"));
            return;
        }
        final ActionListener<S> finishListener = ActionListener.wrap(listener::onResponse, e -> listener.onFailure(finishFailure(task, e)));
        try {
            queue.finish(task.lease, finalState, finishListener);
        } catch (Exception e) {
            finishListener.onFailure(e);
        }
    }

    private synchronized ActiveTask<S> currentTask(TaskImpl<S> execution) {
        final ActiveTask<S> task = active.get(execution.taskId());
        return task != null && task.execution == execution ? task : null;
    }

    private synchronized Exception finishFailure(ActiveTask<S> task, Exception failure) {
        return task.deferredLeaseLoss == null ? failure : task.deferredLeaseLoss;
    }

    private void scheduleNextLeaseCheck() {
        List<ActiveTask<S>> failedTasks = null;
        Exception schedulingFailure = null;
        synchronized (this) {
            if (running == false || active.isEmpty()) {
                cancelNextLeaseCheck();
                return;
            }

            long checkAtMillis = Long.MAX_VALUE;
            for (ActiveTask<S> task : active.values()) {
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
            failedTasks.forEach(task -> task.execution.leaseLost(failure));
        }
    }

    private void checkLeases(long generation) {
        final List<ActiveTask<S>> expired = new ArrayList<>();
        final List<ActiveTask<S>> toRenew = new ArrayList<>();
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
            for (ActiveTask<S> task : active.values()) {
                if (nowMillis >= task.lease.expiryMillis()) {
                    expired.add(task);
                } else if (task.renewalInProgress == false && nowMillis >= task.renewAtMillis) {
                    task.renewalInProgress = true;
                    toRenew.add(task);
                }
            }
        }

        expired.forEach(
            task -> task.execution.leaseLost(new LeaseLostException("lease for task [" + task.lease.taskId() + "] has expired"))
        );
        toRenew.forEach(this::renew);
        scheduleNextLeaseCheck();
    }

    private void renew(ActiveTask<S> task) {
        final Lease lease;
        synchronized (this) {
            if (running == false || active.get(task.execution.taskId()) != task) {
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

    private void leaseRenewed(ActiveTask<S> task, Lease previousLease, Lease renewedLease) {
        Exception invalidLease = null;
        synchronized (this) {
            if (running == false || active.get(task.execution.taskId()) != task) {
                return;
            }
            task.renewalInProgress = false;
            final long nowMillis = threadPool.absoluteTimeInMillis();
            if (task.lease != previousLease
                || renewedLease == null
                || sameLeaseIncarnation(previousLease, renewedLease) == false
                || renewedLease.expiryMillis() <= nowMillis) {
                invalidLease = new LeaseLostException("queue returned an invalid renewed lease for task [" + task.execution.taskId() + "]");
            } else {
                task.lease = renewedLease;
                task.renewAtMillis = renewalTime(nowMillis, renewedLease.expiryMillis());
            }
        }

        if (invalidLease == null) {
            scheduleNextLeaseCheck();
        } else {
            task.execution.leaseLost(invalidLease);
        }
    }

    private void leaseRenewalFailed(ActiveTask<S> task, Exception failure) {
        final boolean loseLease;
        synchronized (this) {
            if (running == false || active.get(task.execution.taskId()) != task) {
                return;
            }
            task.renewalInProgress = false;
            final long nowMillis = threadPool.absoluteTimeInMillis();
            if (failure instanceof LeaseLostException) {
                loseLease = task.execution.isFinishing() == false;
                task.renewAtMillis = task.lease.expiryMillis();
                if (loseLease == false) {
                    task.deferredLeaseLoss = failure;
                }
            } else if (nowMillis >= task.lease.expiryMillis()) {
                loseLease = true;
                failure = new LeaseLostException("lease for task [" + task.execution.taskId() + "] has expired", failure);
            } else {
                loseLease = false;
                final long normalRetryMillis = Math.max(1L, leaseDuration.millis() / 10L);
                final long remainingMillis = task.lease.expiryMillis() - nowMillis;
                task.renewAtMillis = nowMillis + Math.min(normalRetryMillis, Math.max(1L, remainingMillis / 2L));
            }
        }

        if (loseLease) {
            task.execution.leaseLost(failure);
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

    private void release(ActiveTask<S> task) {
        final ActionListener<Void> listener = ActionListener.assertOnce(
            ActionListener.wrap(ignored -> task.execution.releaseCompleted(null), task.execution::releaseCompleted)
        );
        try {
            queue.release(task.lease, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void executionEnded(TaskImpl<S> execution) {
        final boolean refill;
        synchronized (this) {
            final ActiveTask<S> task = active.get(execution.taskId());
            if (task != null && task.execution == execution) {
                active.remove(execution.taskId());
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

    private static final class ActiveTask<S> {
        private final TaskImpl<S> execution;
        private volatile Lease lease;
        private long renewAtMillis;
        private boolean renewalInProgress;
        private Exception deferredLeaseLoss;

        private ActiveTask(TaskImpl<S> execution, Lease lease, long renewAtMillis) {
            this.execution = execution;
            this.lease = lease;
            this.renewAtMillis = renewAtMillis;
        }
    }
}
