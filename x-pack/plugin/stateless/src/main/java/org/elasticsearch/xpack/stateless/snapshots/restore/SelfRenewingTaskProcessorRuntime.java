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

    private record LocalState<T>(T state, boolean closed, boolean terminalUpdate, ActionListener<T> updateListener) {}

    private record LeaseState(Lease lease, boolean renewalInProgress) {}

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
        private final AtomicReference<LeaseState> leaseState;
        private final AtomicReference<LocalState<S>> localState;

        private ActiveTask(S state, Lease lease) {
            this.taskId = lease.taskId();
            this.leaseState = new AtomicReference<>(new LeaseState(lease, false));
            this.localState = new AtomicReference<>(new LocalState<>(state, false, false, null));
        }

        private String taskId() {
            return taskId;
        }

        private boolean isClosed() {
            return localState.get().closed();
        }

        private void start() {
            scheduleLease(leaseState.get().lease());
            startExecution(this);
        }

        private void scheduleLease(Lease lease) {
            final long nowMillis = threadPool.absoluteTimeInMillis();
            final long remainingMillis = lease.expiryMillis() - nowMillis;
            if (remainingMillis <= 0L) {
                leaseLost(new LeaseLostException("lease for task [" + taskId + "] has expired"));
                return;
            }

            try {
                threadPool.schedule(
                    () -> startRenewal(lease),
                    TimeValue.timeValueMillis(Math.max(1L, remainingMillis / 2L)),
                    threadPool.generic()
                );
                threadPool.schedule(() -> leaseExpired(lease), TimeValue.timeValueMillis(remainingMillis), threadPool.generic());
            } catch (Exception e) {
                leaseLost(new LeaseLostException("could not schedule lease management for task [" + taskId + "]", e));
            }
        }

        private void startRenewal(Lease lease) {
            if (isClosed()) {
                return;
            }
            final LeaseState current = leaseState.get();
            if (current.lease() != lease || current.renewalInProgress()) {
                return;
            }
            if (leaseState.compareAndSet(current, new LeaseState(lease, true)) == false) {
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
            if (isClosed()) {
                return;
            }
            final long nowMillis = threadPool.absoluteTimeInMillis();
            final LeaseState current = leaseState.get();
            if (current.lease() != previousLease || current.renewalInProgress() == false) {
                return;
            }
            if (renewedLease == null
                || sameLeaseIncarnation(previousLease, renewedLease) == false
                || renewedLease.expiryMillis() <= nowMillis) {
                leaseLost(new LeaseLostException("queue returned an invalid renewed lease for task [" + taskId + "]"));
                return;
            }
            if (leaseState.compareAndSet(current, new LeaseState(renewedLease, false))) {
                scheduleLease(renewedLease);
            }
        }

        private void leaseRenewalFailed(Lease lease, Exception failure) {
            if (isClosed()) {
                return;
            }
            final LeaseState current = leaseState.get();
            if (current.lease() != lease || current.renewalInProgress() == false) {
                return;
            }
            if (failure instanceof LeaseLostException || threadPool.absoluteTimeInMillis() >= lease.expiryMillis()) {
                final Exception leaseFailure = failure instanceof LeaseLostException
                    ? failure
                    : new LeaseLostException("lease for task [" + taskId + "] has expired", failure);
                leaseLost(leaseFailure);
                return;
            }
            if (leaseState.compareAndSet(current, new LeaseState(lease, false))) {
                final long nowMillis = threadPool.absoluteTimeInMillis();
                final long normalRetryMillis = Math.max(1L, leaseDuration.millis() / 10L);
                final long remainingMillis = lease.expiryMillis() - nowMillis;
                final long retryMillis = Math.clamp(remainingMillis / 2L, 1L, normalRetryMillis);
                try {
                    threadPool.schedule(() -> startRenewal(lease), TimeValue.timeValueMillis(retryMillis), threadPool.generic());
                } catch (Exception e) {
                    leaseLost(new LeaseLostException("could not schedule lease renewal for task [" + taskId + "]", e));
                }
            }
        }

        private void leaseExpired(Lease lease) {
            if (leaseState.get().lease() == lease) {
                leaseLost(new LeaseLostException("lease for task [" + taskId + "] has expired"));
            }
        }

        @Override
        public S state() {
            return localState.get().state();
        }

        @Override
        public void update(S newState, boolean terminal, ActionListener<S> listener) {
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

            final Lease lease = leaseState.get().lease();
            final ActionListener<S> operationListener = ActionListener.assertOnce(
                ActionListener.wrap(
                    persistedState -> stateChangeCompleted(pendingUpdate, persistedState),
                    failure -> stateChangeFailed(pendingUpdate, failure)
                )
            );
            try {
                queue.update(lease, newState, terminal, operationListener);
            } catch (Exception e) {
                operationListener.onFailure(e);
            }
        }

        private void stateChangeCompleted(LocalState<S> pendingUpdate, S persistedState) {
            final LocalState<S> completed = new LocalState<>(persistedState, pendingUpdate.terminalUpdate(), false, null);
            if (localState.compareAndSet(pendingUpdate, completed) == false) {
                return;
            }
            if (completed.closed()) {
                activeTasks.remove(this);
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

        private void stopAndRelease() {
            final Lease lease = leaseState.get().lease();
            if (closeAndCancel(new LeaseLostException("runtime stopped while processing task [" + taskId + "]"))) {
                release(lease);
            }
        }

        private void leaseLost(Exception failure) {
            closeAndCancel(failure);
        }

        private boolean closeAndCancel(Exception failure) {
            final LocalState<S> previous = localState.getAndUpdate(
                current -> current.closed() ? current : new LocalState<>(current.state(), true, false, null)
            );
            if (previous.closed()) {
                return false;
            }

            activeTasks.remove(this);
            cancelTask(this);
            if (previous.updateListener() != null) {
                try {
                    previous.updateListener().onFailure(failure);
                } catch (Exception e) {
                    logger.warn(() -> "state-change listener failed while handling lease loss for task [" + taskId + "]", e);
                }
            }
            return true;
        }
    }
}
