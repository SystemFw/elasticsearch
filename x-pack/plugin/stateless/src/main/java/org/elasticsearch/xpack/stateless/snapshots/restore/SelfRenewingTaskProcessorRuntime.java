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

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * Alternative task runtime in which each active task manages its own lease renewal and expiry scheduling.
 */
public final class SelfRenewingTaskProcessorRuntime<S> extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(SelfRenewingTaskProcessorRuntime.class);

    private final TaskQueue<S> queue;
    private final Task<S> processor;
    private final ThreadPool threadPool;
    private final Executor processorExecutor;
    private final String workerId;
    private final int maxConcurrentTasks;
    private final TimeValue leaseDuration;
    private final TimeValue claimInterval;
    private final Set<ActiveTask> activeTasks = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean claimInProgress = new AtomicBoolean();

    private volatile boolean running;
    private volatile Scheduler.Cancellable claimPoller;

    private record TaskState<T>(
        T state,
        Lease lease,
        boolean closed,
        boolean renewalInProgress,
        long updateGeneration,
        boolean terminalUpdate,
        ActionListener<T> updateListener,
        Scheduler.Cancellable renewalTimer,
        Scheduler.Cancellable expiryTimer
    ) {

        private TaskState<T> withRenewalTimer(Scheduler.Cancellable timer) {
            return new TaskState<>(
                state,
                lease,
                closed,
                renewalInProgress,
                updateGeneration,
                terminalUpdate,
                updateListener,
                timer,
                expiryTimer
            );
        }

        private TaskState<T> withExpiryTimer(Scheduler.Cancellable timer) {
            return new TaskState<>(
                state,
                lease,
                closed,
                renewalInProgress,
                updateGeneration,
                terminalUpdate,
                updateListener,
                renewalTimer,
                timer
            );
        }

        private TaskState<T> renewalStarted() {
            return new TaskState<>(state, lease, closed, true, updateGeneration, terminalUpdate, updateListener, null, expiryTimer);
        }

        private TaskState<T> renewed(Lease renewedLease) {
            return new TaskState<>(state, renewedLease, closed, false, updateGeneration, terminalUpdate, updateListener, null, null);
        }

        private TaskState<T> renewalRetryPending() {
            return new TaskState<>(state, lease, closed, false, updateGeneration, terminalUpdate, updateListener, null, expiryTimer);
        }

        private TaskState<T> updateStarted(boolean terminal, ActionListener<T> listener) {
            return new TaskState<>(
                state,
                lease,
                closed,
                renewalInProgress,
                updateGeneration + 1L,
                terminal,
                listener,
                renewalTimer,
                expiryTimer
            );
        }

        private TaskState<T> updateCompleted(T persistedState) {
            return new TaskState<>(
                persistedState,
                lease,
                terminalUpdate,
                terminalUpdate ? false : renewalInProgress,
                updateGeneration,
                false,
                null,
                terminalUpdate ? null : renewalTimer,
                terminalUpdate ? null : expiryTimer
            );
        }

        private TaskState<T> updateFailed() {
            return new TaskState<>(state, lease, closed, renewalInProgress, updateGeneration, false, null, renewalTimer, expiryTimer);
        }

        private TaskState<T> close() {
            return new TaskState<>(state, lease, true, false, updateGeneration, false, null, null, null);
        }
    }

    /** Creates a runtime for one task type and processor. */
    public SelfRenewingTaskProcessorRuntime(
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
        running = true;
        claimPoller = threadPool.scheduleWithFixedDelay(this::claimAvailableTasks, claimInterval, threadPool.generic());
        claimAvailableTasks();
    }

    @Override
    protected void doStop() {
        running = false;
        final Scheduler.Cancellable poller = claimPoller;
        if (poller != null) {
            poller.cancel();
        }
        for (ActiveTask task : List.copyOf(activeTasks)) {
            task.stopAndRelease();
        }
    }

    @Override
    protected void doClose() {}

    private void claimAvailableTasks() {
        if (running == false || claimInProgress.compareAndSet(false, true) == false) {
            return;
        }

        final int capacity = maxConcurrentTasks - activeTasks.size();
        if (capacity <= 0) {
            claimInProgress.set(false);
            return;
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
        final long nowMillis = threadPool.absoluteTimeInMillis();
        try {
            for (Tuple<S, Lease> claimedTask : claimedTasks) {
                final Lease lease = claimedTask.v2();
                if (running == false || lease.expiryMillis() <= nowMillis) {
                    release(lease);
                    continue;
                }

                final var task = new ActiveTask(claimedTask.v1(), lease);
                activeTasks.add(task);
                if (running) {
                    task.start();
                } else {
                    task.stopAndRelease();
                }
            }
        } finally {
            claimInProgress.set(false);
        }
    }

    private void claimFailed(Exception failure) {
        claimInProgress.set(false);
        if (running) {
            logger.debug("failed to claim queued tasks", failure);
        }
    }

    private void startExecution(ActiveTask task) {
        if (task.isClosed()) {
            return;
        }
        try {
            processorExecutor.execute(() -> {
                if (task.isClosed()) {
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

    private void release(Lease lease) {
        final ActionListener<Void> listener = ActionListener.wrap(
            ignored -> {},
            failure -> logger.debug(() -> "failed to release lease for task [" + lease.taskId() + "]", failure)
        );
        try {
            queue.release(lease, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private static boolean sameLeaseIncarnation(Lease current, Lease renewed) {
        return current.taskId().equals(renewed.taskId())
            && current.ownerId().equals(renewed.ownerId())
            && current.fencingToken() == renewed.fencingToken();
    }

    // Exposed for comparison tests.
    int activeTaskCount() {
        return activeTasks.size();
    }

    private final class ActiveTask implements TaskHandle<S> {

        private final String taskId;
        private final AtomicReference<TaskState<S>> taskState;

        private ActiveTask(S state, Lease lease) {
            this.taskId = lease.taskId();
            this.taskState = new AtomicReference<>(new TaskState<>(state, lease, false, false, 0L, false, null, null, null));
        }

        private String taskId() {
            return taskId;
        }

        private boolean isClosed() {
            return taskState.get().closed();
        }

        private void start() {
            scheduleLease(taskState.get().lease());
            startExecution(this);
        }

        private void scheduleLease(Lease lease) {
            final long nowMillis = threadPool.absoluteTimeInMillis();
            final long remainingMillis = lease.expiryMillis() - nowMillis;
            if (remainingMillis <= 0L) {
                leaseLost(state -> state.lease() == lease, new LeaseLostException("lease for task [" + taskId + "] has expired"));
                return;
            }

            try {
                final Scheduler.Cancellable renewalTimer = threadPool.schedule(
                    () -> startRenewal(lease),
                    TimeValue.timeValueMillis(Math.max(1L, remainingMillis / 2L)),
                    threadPool.generic()
                );
                if (installRenewalTimer(lease, renewalTimer) == false) {
                    return;
                }
                final Scheduler.Cancellable expiryTimer = threadPool.schedule(
                    () -> leaseExpired(lease),
                    TimeValue.timeValueMillis(remainingMillis),
                    threadPool.generic()
                );
                installExpiryTimer(lease, expiryTimer);
            } catch (Exception e) {
                leaseLost(
                    state -> state.lease() == lease,
                    new LeaseLostException("could not schedule lease management for task [" + taskId + "]", e)
                );
            }
        }

        private boolean installRenewalTimer(Lease lease, Scheduler.Cancellable timer) {
            final TaskState<S> previous = taskState.getAndUpdate(current -> {
                if (current.closed() || current.lease() != lease || current.renewalTimer() != null) {
                    return current;
                }
                return current.withRenewalTimer(timer);
            });
            if (previous.closed() || previous.lease() != lease || previous.renewalTimer() != null) {
                timer.cancel();
                return false;
            }
            return true;
        }

        private boolean installExpiryTimer(Lease lease, Scheduler.Cancellable timer) {
            final TaskState<S> previous = taskState.getAndUpdate(current -> {
                if (current.closed() || current.lease() != lease || current.expiryTimer() != null) {
                    return current;
                }
                return current.withExpiryTimer(timer);
            });
            if (previous.closed() || previous.lease() != lease || previous.expiryTimer() != null) {
                timer.cancel();
                return false;
            }
            return true;
        }

        private void startRenewal(Lease lease) {
            final TaskState<S> previous = taskState.getAndUpdate(current -> {
                if (current.closed() || current.lease() != lease || current.renewalInProgress()) {
                    return current;
                }
                return current.renewalStarted();
            });
            if (previous.closed() || previous.lease() != lease || previous.renewalInProgress()) {
                return;
            }

            final ActionListener<Lease> listener = ActionListener.assertOnce(
                ActionListener.wrap(renewedLease -> leaseRenewed(lease, renewedLease), failure -> leaseRenewalFailed(lease, failure))
            );
            try {
                queue.renew(lease, leaseDuration, listener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        private void leaseRenewed(Lease previousLease, Lease renewedLease) {
            final long nowMillis = threadPool.absoluteTimeInMillis();
            if (renewedLease == null
                || sameLeaseIncarnation(previousLease, renewedLease) == false
                || renewedLease.expiryMillis() <= nowMillis) {
                leaseLost(
                    state -> state.lease() == previousLease && state.renewalInProgress(),
                    new LeaseLostException("queue returned an invalid renewed lease for task [" + taskId + "]")
                );
                return;
            }

            final TaskState<S> previous = taskState.getAndUpdate(current -> {
                if (current.closed() || current.lease() != previousLease || current.renewalInProgress() == false) {
                    return current;
                }
                return current.renewed(renewedLease);
            });
            if (previous.closed() || previous.lease() != previousLease || previous.renewalInProgress() == false) {
                return;
            }
            cancelTimer(previous.renewalTimer());
            cancelTimer(previous.expiryTimer());
            scheduleLease(renewedLease);
        }

        private void leaseRenewalFailed(Lease lease, Exception failure) {
            final long nowMillis = threadPool.absoluteTimeInMillis();
            if (failure instanceof LeaseLostException || nowMillis >= lease.expiryMillis()) {
                final Exception leaseFailure = failure instanceof LeaseLostException
                    ? failure
                    : new LeaseLostException("lease for task [" + taskId + "] has expired", failure);
                leaseLost(state -> state.lease() == lease && state.renewalInProgress(), leaseFailure);
                return;
            }

            final TaskState<S> previous = taskState.getAndUpdate(current -> {
                if (current.closed() || current.lease() != lease || current.renewalInProgress() == false) {
                    return current;
                }
                return current.renewalRetryPending();
            });
            if (previous.closed() || previous.lease() != lease || previous.renewalInProgress() == false) {
                return;
            }

            final long normalRetryMillis = Math.max(1L, leaseDuration.millis() / 10L);
            final long remainingMillis = lease.expiryMillis() - nowMillis;
            final long retryMillis = Math.clamp(remainingMillis / 2L, 1L, normalRetryMillis);
            try {
                final Scheduler.Cancellable renewalTimer = threadPool.schedule(
                    () -> startRenewal(lease),
                    TimeValue.timeValueMillis(retryMillis),
                    threadPool.generic()
                );
                installRenewalTimer(lease, renewalTimer);
            } catch (Exception e) {
                leaseLost(
                    state -> state.lease() == lease,
                    new LeaseLostException("could not schedule lease renewal for task [" + taskId + "]", e)
                );
            }
        }

        private void leaseExpired(Lease lease) {
            leaseLost(state -> state.lease() == lease, new LeaseLostException("lease for task [" + taskId + "] has expired"));
        }

        @Override
        public S state() {
            return taskState.get().state();
        }

        @Override
        public void update(S newState, boolean terminal, ActionListener<S> listener) {
            final TaskState<S> previous = taskState.getAndUpdate(current -> {
                if (current.closed() || current.updateListener() != null) {
                    return current;
                }
                return current.updateStarted(terminal, listener);
            });
            if (previous.closed()) {
                listener.onFailure(new LeaseLostException("lease for task [" + taskId + "] is no longer active"));
                return;
            }
            if (previous.updateListener() != null) {
                listener.onFailure(new IllegalStateException("task [" + taskId + "] already has a persistent state change in progress"));
                return;
            }

            final long updateGeneration = previous.updateGeneration() + 1L;
            final Lease lease = previous.lease();
            final ActionListener<S> operationListener = ActionListener.assertOnce(
                ActionListener.wrap(
                    persistedState -> stateChangeCompleted(updateGeneration, persistedState),
                    failure -> stateChangeFailed(updateGeneration, failure)
                )
            );
            if (threadPool.absoluteTimeInMillis() >= lease.expiryMillis()) {
                operationListener.onFailure(new LeaseLostException("lease for task [" + taskId + "] has expired"));
            } else {
                try {
                    queue.update(lease, newState, terminal, operationListener);
                } catch (Exception e) {
                    operationListener.onFailure(e);
                }
            }
        }

        private void stateChangeCompleted(long updateGeneration, S persistedState) {
            final TaskState<S> previous = taskState.getAndUpdate(current -> {
                if (current.closed() || current.updateListener() == null || current.updateGeneration() != updateGeneration) {
                    return current;
                }
                return current.updateCompleted(persistedState);
            });
            if (previous.closed() || previous.updateListener() == null || previous.updateGeneration() != updateGeneration) {
                return;
            }
            if (previous.terminalUpdate()) {
                cancelTimer(previous.renewalTimer());
                cancelTimer(previous.expiryTimer());
                activeTasks.remove(this);
            }
            previous.updateListener().onResponse(persistedState);
        }

        private void stateChangeFailed(long updateGeneration, Exception failure) {
            if (failure instanceof LeaseLostException) {
                leaseLost(state -> state.updateListener() != null && state.updateGeneration() == updateGeneration, failure);
                return;
            }

            final TaskState<S> previous = taskState.getAndUpdate(current -> {
                if (current.closed() || current.updateListener() == null || current.updateGeneration() != updateGeneration) {
                    return current;
                }
                return current.updateFailed();
            });
            if (previous.closed() == false && previous.updateListener() != null && previous.updateGeneration() == updateGeneration) {
                previous.updateListener().onFailure(failure);
            }
        }

        private void processorFailed(Exception failure) {
            logger.warn(() -> "task processor failed unexpectedly for task [" + taskId + "]", failure);
            leaseLost(state -> true, new LeaseLostException("task processor failed for task [" + taskId + "]", failure));
        }

        private void stopAndRelease() {
            final TaskState<S> previous = closeAndCancel(
                state -> true,
                new LeaseLostException("runtime stopped while processing task [" + taskId + "]")
            );
            if (previous != null) {
                release(previous.lease());
            }
        }

        private void leaseLost(Predicate<TaskState<S>> condition, Exception failure) {
            closeAndCancel(condition, failure);
        }

        private TaskState<S> closeAndCancel(Predicate<TaskState<S>> condition, Exception failure) {
            final TaskState<S> previous = taskState.getAndUpdate(current -> {
                if (current.closed() || condition.test(current) == false) {
                    return current;
                }
                return current.close();
            });
            if (previous.closed() || condition.test(previous) == false) {
                return null;
            }

            activeTasks.remove(this);
            cancelTimer(previous.renewalTimer());
            cancelTimer(previous.expiryTimer());
            cancelTask(this);
            if (previous.updateListener() != null) {
                try {
                    previous.updateListener().onFailure(failure);
                } catch (Exception e) {
                    logger.warn(() -> "state-change listener failed while handling lease loss for task [" + taskId + "]", e);
                }
            }
            return previous;
        }

        private void cancelTimer(Scheduler.Cancellable timer) {
            if (timer != null) {
                timer.cancel();
            }
        }
    }
}
