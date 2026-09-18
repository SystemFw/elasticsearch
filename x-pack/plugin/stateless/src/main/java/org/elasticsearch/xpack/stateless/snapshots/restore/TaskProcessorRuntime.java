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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.snapshots.restore.Task.TaskHandle;
import org.elasticsearch.xpack.stateless.snapshots.restore.TaskQueue.Lease;
import org.elasticsearch.xpack.stateless.snapshots.restore.TaskQueue.LeaseLostException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
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
    private final ConcurrentHashMap<String, ActiveTask> active = new ConcurrentHashMap<>();

    // Scheduled callbacks validate their generation against this state instead of being cancelled.
    private final AtomicReference<RuntimeState> runtimeState = new AtomicReference<>(RuntimeState.initial());

    private record LocalState<T>(T state, boolean closed, boolean terminalUpdate, ActionListener<T> updateListener) {}

    private record LeaseState(Lease lease, long renewAtMillis, boolean renewalInProgress) {}

    private record RuntimeState(
        boolean running,
        boolean claimInProgress,
        boolean claimScheduled,
        long claimGeneration,
        long leaseCheckAtMillis,
        long leaseCheckGeneration
    ) {

        private static RuntimeState initial() {
            return new RuntimeState(false, false, false, 0L, Long.MAX_VALUE, 0L);
        }

        private RuntimeState started() {
            return new RuntimeState(true, claimInProgress, claimScheduled, claimGeneration, leaseCheckAtMillis, leaseCheckGeneration);
        }

        private RuntimeState stopped() {
            return new RuntimeState(false, claimInProgress, false, claimGeneration + 1L, Long.MAX_VALUE, leaseCheckGeneration + 1L);
        }

        private RuntimeState claimStarted() {
            return new RuntimeState(true, true, false, claimGeneration + 1L, leaseCheckAtMillis, leaseCheckGeneration);
        }

        private RuntimeState scheduleClaim() {
            return new RuntimeState(true, false, true, claimGeneration + 1L, leaseCheckAtMillis, leaseCheckGeneration);
        }

        private RuntimeState scheduledClaimStarted() {
            return new RuntimeState(true, true, false, claimGeneration, leaseCheckAtMillis, leaseCheckGeneration);
        }

        private RuntimeState claimCompleted() {
            return new RuntimeState(running, false, claimScheduled, claimGeneration, leaseCheckAtMillis, leaseCheckGeneration);
        }

        private RuntimeState claimScheduleCleared() {
            return new RuntimeState(running, claimInProgress, false, claimGeneration, leaseCheckAtMillis, leaseCheckGeneration);
        }

        private RuntimeState leaseCheckScheduled(long checkAtMillis) {
            return new RuntimeState(running, claimInProgress, claimScheduled, claimGeneration, checkAtMillis, leaseCheckGeneration + 1L);
        }

        private RuntimeState leaseCheckCleared() {
            return new RuntimeState(running, claimInProgress, claimScheduled, claimGeneration, Long.MAX_VALUE, leaseCheckGeneration);
        }
    }

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
        runtimeState.getAndUpdate(RuntimeState::started);
        fillCapacity();
    }

    @Override
    protected void doStop() {
        runtimeState.getAndUpdate(RuntimeState::stopped);
        for (ActiveTask task : active.values()) {
            if (task.runtimeStopped()) {
                release(task);
            }
        }
    }

    @Override
    protected void doClose() {}

    private void fillCapacity() {
        final int capacity = maxConcurrentTasks - active.size();
        if (capacity <= 0) {
            return;
        }

        final RuntimeState previous = runtimeState.getAndUpdate(state -> {
            if (state.running() == false || state.claimInProgress()) {
                return state;
            }
            return state.claimStarted();
        });
        if (previous.running() == false || previous.claimInProgress()) {
            return;
        }

        claim(capacity);
    }

    private void claim(int capacity) {
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

        final long nowMillis = threadPool.absoluteTimeInMillis();
        for (Tuple<S, Lease> task : claimedTasks) {
            final Lease lease = task.v2();
            if (runtimeState.get().running() && active.size() < maxConcurrentTasks && lease.expiryMillis() > nowMillis) {
                final var activeTask = new ActiveTask(task.v1(), lease, renewalTime(nowMillis, lease.expiryMillis()));
                if (active.putIfAbsent(lease.taskId(), activeTask) == null) {
                    if (runtimeState.get().running()) {
                        toStart.add(activeTask);
                    } else if (activeTask.runtimeStopped()) {
                        release(activeTask);
                    }
                    continue;
                }
            }
            toRelease.add(lease);
        }
        runtimeState.getAndUpdate(RuntimeState::claimCompleted);

        toRelease.forEach(this::releaseUnstartedLease);
        scheduleNextLeaseCheck();
        toStart.forEach(this::startExecution);
        scheduleNextClaim();
    }

    private void claimFailed(Exception failure) {
        final RuntimeState previous = runtimeState.getAndUpdate(RuntimeState::claimCompleted);
        if (previous.running() == false) {
            return;
        }
        logger.debug("failed to claim queued tasks", failure);
        scheduleNextClaim();
    }

    private void scheduleNextClaim() {
        if (active.size() >= maxConcurrentTasks) {
            return;
        }

        final RuntimeState previous = runtimeState.getAndUpdate(state -> {
            if (state.running() == false || state.claimInProgress() || state.claimScheduled()) {
                return state;
            }
            return state.scheduleClaim();
        });
        if (previous.running() == false || previous.claimInProgress() || previous.claimScheduled()) {
            return;
        }

        final long generation = previous.claimGeneration() + 1L;
        try {
            threadPool.schedule(() -> runScheduledClaim(generation), claimInterval, threadPool.generic());
        } catch (Exception e) {
            runtimeState.getAndUpdate(state -> {
                if (state.claimScheduled() && state.claimGeneration() == generation) {
                    return state.claimScheduleCleared();
                }
                return state;
            });
            logger.debug("failed to schedule the next task claim", e);
        }
    }

    private void runScheduledClaim(long generation) {
        final int capacity = maxConcurrentTasks - active.size();
        final RuntimeState previous = runtimeState.getAndUpdate(state -> {
            if (state.claimScheduled() == false || state.claimGeneration() != generation) {
                return state;
            }
            if (state.running() && state.claimInProgress() == false && capacity > 0) {
                return state.scheduledClaimStarted();
            }
            return state.claimScheduleCleared();
        });

        if (previous.running()
            && previous.claimInProgress() == false
            && previous.claimScheduled()
            && previous.claimGeneration() == generation
            && capacity > 0) {
            claim(capacity);
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
            queue.update(task.leaseState.get().lease(), newState, terminal, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private ActiveTask currentTask(ActiveTask handle) {
        final ActiveTask task = active.get(handle.taskId());
        return task == handle ? task : null;
    }

    private void scheduleNextLeaseCheck() {
        if (runtimeState.get().running() == false || active.isEmpty()) {
            return;
        }

        long checkAtMillis = Long.MAX_VALUE;
        for (ActiveTask task : active.values()) {
            final LeaseState leaseState = task.leaseState.get();
            final long taskCheckAt = leaseState.renewalInProgress()
                ? leaseState.lease().expiryMillis()
                : Math.min(leaseState.renewAtMillis(), leaseState.lease().expiryMillis());
            checkAtMillis = Math.min(checkAtMillis, taskCheckAt);
        }
        if (checkAtMillis == Long.MAX_VALUE) {
            return;
        }

        final long nextCheckAtMillis = checkAtMillis;
        final RuntimeState previous = runtimeState.getAndUpdate(state -> {
            if (state.running() == false || state.leaseCheckAtMillis() <= nextCheckAtMillis) {
                return state;
            }
            return state.leaseCheckScheduled(nextCheckAtMillis);
        });
        if (previous.running() == false || previous.leaseCheckAtMillis() <= nextCheckAtMillis) {
            return;
        }

        final long generation = previous.leaseCheckGeneration() + 1L;
        final long delayMillis = Math.max(1L, nextCheckAtMillis - threadPool.absoluteTimeInMillis());
        try {
            threadPool.schedule(() -> checkLeases(generation), TimeValue.timeValueMillis(delayMillis), threadPool.generic());
        } catch (Exception e) {
            final RuntimeState failedSchedule = runtimeState.getAndUpdate(state -> {
                if (state.leaseCheckGeneration() == generation) {
                    return state.leaseCheckCleared();
                }
                return state;
            });
            if (failedSchedule.running() && failedSchedule.leaseCheckGeneration() == generation) {
                final var failure = new LeaseLostException("could not schedule lease management", e);
                active.values().forEach(task -> task.leaseLost(failure));
            }
        }
    }

    private void checkLeases(long generation) {
        final RuntimeState previous = runtimeState.getAndUpdate(state -> {
            if (state.running() == false || state.leaseCheckGeneration() != generation) {
                return state;
            }
            return state.leaseCheckCleared();
        });
        if (previous.running() == false || previous.leaseCheckGeneration() != generation) {
            return;
        }

        final List<ActiveTask> expired = new ArrayList<>();
        final List<Tuple<ActiveTask, Lease>> toRenew = new ArrayList<>();
        final long nowMillis = threadPool.absoluteTimeInMillis();
        for (ActiveTask task : active.values()) {
            final LeaseState leaseState = task.leaseState.get();
            if (nowMillis >= leaseState.lease().expiryMillis()) {
                expired.add(task);
            } else if (leaseState.renewalInProgress() == false && nowMillis >= leaseState.renewAtMillis()) {
                final LeaseState renewing = new LeaseState(leaseState.lease(), leaseState.renewAtMillis(), true);
                if (task.leaseState.compareAndSet(leaseState, renewing)) {
                    toRenew.add(new Tuple<>(task, leaseState.lease()));
                }
            }
        }

        expired.forEach(
            task -> task.leaseLost(new LeaseLostException("lease for task [" + task.leaseState.get().lease().taskId() + "] has expired"))
        );
        toRenew.forEach(task -> renew(task.v1(), task.v2()));
        scheduleNextLeaseCheck();
    }

    private void renew(ActiveTask task, Lease lease) {
        if (runtimeState.get().running() == false || active.get(task.taskId()) != task) {
            return;
        }
        final ActionListener<Lease> listener = ActionListener.assertOnce(
            ActionListener.wrap(renewedLease -> leaseRenewed(task, lease, renewedLease), e -> leaseRenewalFailed(task, lease, e))
        );
        try {
            queue.renew(lease, leaseDuration, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void leaseRenewed(ActiveTask task, Lease previousLease, Lease renewedLease) {
        if (runtimeState.get().running() == false || active.get(task.taskId()) != task) {
            return;
        }

        final long nowMillis = threadPool.absoluteTimeInMillis();
        final boolean validLease = renewedLease != null
            && sameLeaseIncarnation(previousLease, renewedLease)
            && renewedLease.expiryMillis() > nowMillis;
        final LeaseState previous = task.leaseState.getAndUpdate(leaseState -> {
            if (leaseState.lease() != previousLease || leaseState.renewalInProgress() == false) {
                return leaseState;
            }
            if (validLease) {
                return new LeaseState(renewedLease, renewalTime(nowMillis, renewedLease.expiryMillis()), false);
            }
            return new LeaseState(leaseState.lease(), leaseState.renewAtMillis(), false);
        });
        if (previous.lease() != previousLease || previous.renewalInProgress() == false) {
            return;
        }

        if (validLease == false) {
            task.leaseLost(new LeaseLostException("queue returned an invalid renewed lease for task [" + task.taskId() + "]"));
            return;
        }
        scheduleNextLeaseCheck();
    }

    private void leaseRenewalFailed(ActiveTask task, Lease previousLease, Exception failure) {
        if (runtimeState.get().running() == false || active.get(task.taskId()) != task) {
            return;
        }

        final long nowMillis = threadPool.absoluteTimeInMillis();
        final boolean fenced = failure instanceof LeaseLostException;
        final LeaseState previous = task.leaseState.getAndUpdate(leaseState -> {
            if (leaseState.lease() != previousLease || leaseState.renewalInProgress() == false) {
                return leaseState;
            }
            if (fenced) {
                return new LeaseState(leaseState.lease(), leaseState.lease().expiryMillis(), false);
            }
            if (nowMillis >= leaseState.lease().expiryMillis()) {
                return new LeaseState(leaseState.lease(), leaseState.renewAtMillis(), false);
            }
            final long normalRetryMillis = Math.max(1L, leaseDuration.millis() / 10L);
            final long remainingMillis = leaseState.lease().expiryMillis() - nowMillis;
            final long renewAtMillis = nowMillis + Math.clamp(remainingMillis / 2L, 1L, normalRetryMillis);
            return new LeaseState(leaseState.lease(), renewAtMillis, false);
        });
        if (previous.lease() != previousLease || previous.renewalInProgress() == false) {
            return;
        }

        final boolean loseLease = fenced || nowMillis >= previous.lease().expiryMillis();
        if (loseLease) {
            final Exception leaseFailure = fenced
                ? failure
                : new LeaseLostException("lease for task [" + task.taskId() + "] has expired", failure);
            task.leaseLost(leaseFailure);
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
            queue.release(task.leaseState.get().lease(), listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void taskEnded(ActiveTask task) {
        if (active.remove(task.taskId(), task)) {
            scheduleNextLeaseCheck();
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
    int activeTaskCount() {
        return active.size();
    }

    /** Holds the runtime and processor-facing state for one lease incarnation. */
    private final class ActiveTask implements TaskHandle<S> {

        private final String taskId;

        // Lease scheduling and processor-visible state change independently.
        private final AtomicReference<LeaseState> leaseState;
        private final AtomicReference<LocalState<S>> localState;

        private ActiveTask(S state, Lease lease, long renewAtMillis) {
            this.taskId = lease.taskId();
            this.leaseState = new AtomicReference<>(new LeaseState(lease, renewAtMillis, false));
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
