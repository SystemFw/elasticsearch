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

    private final Object mutex = new Object();
    private final List<ActiveTask> tasks = new ArrayList<>();
    private boolean running;
    private boolean claimInProgress;
    private long nextClaimAtMillis = Long.MAX_VALUE;
    private long wakeAtMillis = Long.MAX_VALUE;
    private long wakeGeneration;

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
        synchronized (mutex) {
            running = true;
            nextClaimAtMillis = threadPool.absoluteTimeInMillis();
        }
        reconcile();
    }

    @Override
    protected void doStop() {
        final List<ActiveTask> stoppedTasks;
        synchronized (mutex) {
            running = false;
            nextClaimAtMillis = Long.MAX_VALUE;
            wakeAtMillis = Long.MAX_VALUE;
            wakeGeneration++;
            stoppedTasks = List.copyOf(tasks);
            tasks.clear();
        }

        for (ActiveTask task : stoppedTasks) {
            final var failure = new LeaseLostException("runtime stopped while processing task [" + task.taskId() + "]");
            task.leaseLost(failure);
            release(task.lease());
        }
    }

    @Override
    protected void doClose() {}

    /**
     * Reconciles all runtime-owned state under one short critical section. Queue, processor and scheduler calls are dispatched afterwards.
     */
    private void reconcile() {
        final List<Tuple<ActiveTask, Lease>> toRenew = new ArrayList<>();
        final List<Tuple<ActiveTask, Exception>> toCancel = new ArrayList<>();
        int claimCapacity = 0;
        long scheduleAtMillis = Long.MAX_VALUE;
        long scheduleGeneration = 0L;
        final long nowMillis = threadPool.absoluteTimeInMillis();

        synchronized (mutex) {
            if (running == false) {
                return;
            }

            for (int i = tasks.size() - 1; i >= 0; i--) {
                final ActiveTask task = tasks.get(i);
                final LeaseState leaseState = task.leaseState();
                if (task.isClosed()) {
                    tasks.remove(i);
                    nextClaimAtMillis = Math.min(nextClaimAtMillis, nowMillis);
                } else if (nowMillis >= leaseState.lease().expiryMillis()) {
                    tasks.remove(i);
                    nextClaimAtMillis = Math.min(nextClaimAtMillis, nowMillis);
                    toCancel.add(new Tuple<>(task, new LeaseLostException("lease for task [" + task.taskId() + "] has expired")));
                } else if (leaseState.renewalInProgress() == false
                    && nowMillis >= leaseState.renewAtMillis()
                    && task.startRenewal(leaseState)) {
                        toRenew.add(new Tuple<>(task, leaseState.lease()));
                    }
            }

            final int capacity = maxConcurrentTasks - tasks.size();
            if (capacity > 0 && claimInProgress == false && nowMillis >= nextClaimAtMillis) {
                claimInProgress = true;
                nextClaimAtMillis = Long.MAX_VALUE;
                claimCapacity = capacity;
            }

            long nextWakeAtMillis = capacity > 0 && claimInProgress == false ? nextClaimAtMillis : Long.MAX_VALUE;
            for (ActiveTask task : tasks) {
                final LeaseState leaseState = task.leaseState();
                nextWakeAtMillis = Math.min(
                    nextWakeAtMillis,
                    leaseState.renewalInProgress()
                        ? leaseState.lease().expiryMillis()
                        : Math.min(leaseState.renewAtMillis(), leaseState.lease().expiryMillis())
                );
            }
            if (nextWakeAtMillis < wakeAtMillis) {
                wakeAtMillis = nextWakeAtMillis;
                scheduleAtMillis = nextWakeAtMillis;
                scheduleGeneration = ++wakeGeneration;
            }
        }

        for (Tuple<ActiveTask, Exception> cancellation : toCancel) {
            cancellation.v1().leaseLost(cancellation.v2());
        }
        for (Tuple<ActiveTask, Lease> renewal : toRenew) {
            renewal.v1().renew(renewal.v2());
        }
        if (claimCapacity > 0) {
            claim(claimCapacity);
        }
        if (scheduleAtMillis != Long.MAX_VALUE) {
            scheduleWake(scheduleAtMillis, scheduleGeneration);
        }
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
        final List<ActiveTask> toStart = new ArrayList<>();
        final List<Lease> toRelease = new ArrayList<>();
        final long nowMillis = threadPool.absoluteTimeInMillis();

        synchronized (mutex) {
            claimInProgress = false;
            if (running) {
                for (Tuple<S, Lease> claimedTask : claimedTasks) {
                    final Lease lease = claimedTask.v2();
                    if (tasks.size() < maxConcurrentTasks
                        && lease.expiryMillis() > nowMillis
                        && tasks.stream().noneMatch(task -> task.taskId().equals(lease.taskId()))) {
                        final var task = new ActiveTask(claimedTask.v1(), lease, renewalTime(nowMillis, lease.expiryMillis()));
                        tasks.add(task);
                        toStart.add(task);
                    } else {
                        toRelease.add(lease);
                    }
                }
                nextClaimAtMillis = nowMillis + claimInterval.millis();
            } else {
                claimedTasks.forEach(task -> toRelease.add(task.v2()));
            }
        }

        toRelease.forEach(this::release);
        toStart.forEach(this::startExecution);
        reconcile();
    }

    private void claimFailed(Exception failure) {
        synchronized (mutex) {
            claimInProgress = false;
            if (running) {
                nextClaimAtMillis = threadPool.absoluteTimeInMillis() + claimInterval.millis();
            }
        }
        logger.debug("failed to claim queued tasks", failure);
        reconcile();
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

    private void scheduleWake(long atMillis, long generation) {
        final long delayMillis = Math.max(1L, atMillis - threadPool.absoluteTimeInMillis());
        try {
            threadPool.schedule(() -> wake(generation), TimeValue.timeValueMillis(delayMillis), threadPool.generic());
        } catch (Exception e) {
            final List<ActiveTask> affectedTasks = new ArrayList<>();
            synchronized (mutex) {
                if (running && wakeGeneration == generation) {
                    wakeAtMillis = Long.MAX_VALUE;
                    nextClaimAtMillis = Long.MAX_VALUE;
                    affectedTasks.addAll(tasks);
                    tasks.clear();
                }
            }
            final var failure = new LeaseLostException("could not schedule task queue maintenance", e);
            affectedTasks.forEach(task -> task.leaseLost(failure));
        }
    }

    private void wake(long generation) {
        synchronized (mutex) {
            if (running == false || generation != wakeGeneration) {
                return;
            }
            wakeAtMillis = Long.MAX_VALUE;
        }
        reconcile();
    }

    private void update(Lease lease, S newState, boolean terminal, ActionListener<S> listener) {
        try {
            queue.update(lease, newState, terminal, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void closeAndCancel(ActiveTask task, Exception failure) {
        final LocalState<S> previous = task.close();
        if (previous != null) {
            try {
                processor.cancel(task);
            } catch (Exception e) {
                logger.warn(() -> "task processor failed to cancel task [" + task.taskId() + "]", e);
            }
            if (previous.updateListener() != null) {
                try {
                    previous.updateListener().onFailure(failure);
                } catch (Exception e) {
                    logger.warn(() -> "state-change listener failed while handling lease loss for task [" + task.taskId() + "]", e);
                }
            }
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

    private static long renewalTime(long nowMillis, long expiryMillis) {
        return nowMillis + Math.max(1L, (expiryMillis - nowMillis) / 2L);
    }

    // Exposed for tests.
    int activeTaskCount() {
        synchronized (mutex) {
            return tasks.size();
        }
    }

    /** Local state for one active lease incarnation. */
    private final class ActiveTask implements TaskHandle<S> {

        private final String taskId;
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

        private boolean isClosed() {
            return localState.get().closed();
        }

        private LeaseState leaseState() {
            return leaseState.get();
        }

        private Lease lease() {
            return leaseState.get().lease();
        }

        private boolean startRenewal(LeaseState current) {
            return leaseState.compareAndSet(current, new LeaseState(current.lease(), current.renewAtMillis(), true));
        }

        private void renew(Lease lease) {
            if (isClosed()) {
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
            final boolean validLease = renewedLease != null
                && sameLeaseIncarnation(previousLease, renewedLease)
                && renewedLease.expiryMillis() > nowMillis;
            final LeaseState current = leaseState.get();
            if (current.lease() != previousLease || current.renewalInProgress() == false) {
                return;
            }

            final LeaseState updated = validLease
                ? new LeaseState(renewedLease, renewalTime(nowMillis, renewedLease.expiryMillis()), false)
                : new LeaseState(previousLease, current.renewAtMillis(), false);
            if (leaseState.compareAndSet(current, updated) == false) {
                return;
            }

            if (validLease == false) {
                leaseLost(new LeaseLostException("queue returned an invalid renewed lease for task [" + taskId + "]"));
            } else {
                reconcile();
            }
        }

        private void leaseRenewalFailed(Lease previousLease, Exception failure) {
            final long nowMillis = threadPool.absoluteTimeInMillis();
            final LeaseState current = leaseState.get();
            if (current.lease() != previousLease || current.renewalInProgress() == false) {
                return;
            }

            final boolean lost = failure instanceof LeaseLostException || nowMillis >= previousLease.expiryMillis();
            final long retryAtMillis;
            if (lost) {
                retryAtMillis = previousLease.expiryMillis();
            } else {
                final long normalRetryMillis = Math.max(1L, leaseDuration.millis() / 10L);
                final long remainingMillis = previousLease.expiryMillis() - nowMillis;
                retryAtMillis = nowMillis + Math.clamp(remainingMillis / 2L, 1L, normalRetryMillis);
            }
            if (leaseState.compareAndSet(current, new LeaseState(previousLease, retryAtMillis, false)) == false) {
                return;
            }

            if (lost) {
                final Exception leaseFailure = failure instanceof LeaseLostException
                    ? failure
                    : new LeaseLostException("lease for task [" + taskId + "] has expired", failure);
                leaseLost(leaseFailure);
            } else {
                reconcile();
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

            final ActionListener<S> operationListener = ActionListener.assertOnce(
                ActionListener.wrap(
                    persistedState -> stateChangeCompleted(pendingUpdate, persistedState),
                    failure -> stateChangeFailed(pendingUpdate, failure)
                )
            );
            final Lease lease = leaseState.get().lease();
            if (threadPool.absoluteTimeInMillis() >= lease.expiryMillis()) {
                operationListener.onFailure(new LeaseLostException("lease for task [" + taskId + "] has expired"));
            } else {
                TaskProcessorRuntime.this.update(lease, newState, terminal, operationListener);
            }
        }

        private void stateChangeCompleted(LocalState<S> pendingUpdate, S persistedState) {
            final LocalState<S> completed = new LocalState<>(persistedState, pendingUpdate.terminalUpdate(), false, null);
            if (localState.compareAndSet(pendingUpdate, completed) == false) {
                return;
            }

            if (completed.closed()) {
                reconcile();
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

        private void leaseLost(Exception failure) {
            closeAndCancel(this, failure);
            reconcile();
        }

        private LocalState<S> close() {
            final LocalState<S> previous = localState.getAndUpdate(
                current -> current.closed() ? current : new LocalState<>(current.state(), true, false, null)
            );
            return previous.closed() ? null : previous;
        }
    }
}
