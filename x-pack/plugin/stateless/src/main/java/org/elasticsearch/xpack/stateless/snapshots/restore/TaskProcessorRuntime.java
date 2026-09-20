/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.restore;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.util.Result;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.snapshots.restore.TaskQueue.Lease;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Processes tasks claimed from a {@link TaskQueue}, with bounded concurrency and centralized lease management. Instances must be fully
 * constructed before the supplied {@link ClusterService} is started, so that lifecycle callbacks cannot reach a partially initialized
 * subclass.
 */
public abstract class TaskProcessorRuntime<S> {

    /** Access to one leased task. A task may have internal concurrency, but only one call to {@link #update} may be outstanding. */
    public interface TaskHandle<S> {

        /** Returns the state from the latest successful persistent state change. */
        S state();

        /** Persists a state change, making the task terminal when {@code terminal} is {@code true}. */
        void update(S newState, boolean terminal, ActionListener<S> listener);
    }

    /** Starts processing a claimed task. */
    protected abstract void process(TaskHandle<S> task) throws Exception;

    /** Stops processing a task whose lease is no longer owned by this runtime. */
    protected abstract void cancel(TaskHandle<S> task);

    /** Creates a runtime for one task type. */
    protected TaskProcessorRuntime(
        ClusterService clusterService,
        TaskQueue<S> queue,
        ThreadPool threadPool,
        Executor processorExecutor,
        String workerId,
        int maxConcurrentTasks,
        TimeValue leaseDuration,
        TimeValue claimInterval
    ) {
        Objects.requireNonNull(clusterService);
        this.queue = Objects.requireNonNull(queue);
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
        this.renewalBatchWindowMillis = Math.min(claimInterval.millis(), Math.max(1L, leaseDuration.millis() / 10L));
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void afterStart() {
                startProcessing();
            }

            @Override
            public void beforeStop() {
                stopProcessing();
            }
        });
    }

    private static final Logger logger = LogManager.getLogger(TaskProcessorRuntime.class);
    private final TaskQueue<S> queue;
    private final ThreadPool threadPool;
    private final Executor processorExecutor;
    private final String workerId;
    private final int maxConcurrentTasks;
    private final TimeValue leaseDuration;
    private final TimeValue claimInterval;
    private final long renewalBatchWindowMillis;

    private final Object mutex = new Object();
    private final List<ActiveTask> tasks = new ArrayList<>();
    private volatile boolean running;
    private boolean claimInProgress;
    private long nextClaimAtMillis;
    private long wakeAtMillis = Long.MAX_VALUE;
    private volatile int taskCount;

    // Exposed for tests that exercise the runtime independently of ClusterService.
    void startProcessing() {
        running = true;
        reconcile();
    }

    // Exposed for tests that exercise the runtime independently of ClusterService.
    void stopProcessing() {
        running = false;
        reconcile();
    }

    private void reconcile() {
        reconcile(false, List.of(), List.of());
    }

    /**
     * Reconciles all runtime-owned state under one short critical section. Queue, processor and scheduler calls are dispatched afterwards.
     */
    private void reconcile(
        boolean claimCompleted,
        List<Tuple<S, Lease>> claimedTasks,
        List<Tuple<ActiveTask, Result<Lease, Exception>>> renewals
    ) {
        final List<Tuple<ActiveTask, Lease>> toRenew = new ArrayList<>();
        final List<Tuple<ActiveTask, Exception>> toCancel = new ArrayList<>();
        final List<Lease> toRelease = new ArrayList<>();
        final List<ActiveTask> toStart = new ArrayList<>();
        int claimCapacity = 0;
        long scheduleAtMillis = Long.MAX_VALUE;
        final long nowMillis = threadPool.absoluteTimeInMillis();

        synchronized (mutex) {
            if (nowMillis >= wakeAtMillis) {
                wakeAtMillis = Long.MAX_VALUE;
            }

            if (running == false) {
                claimedTasks.forEach(task -> toRelease.add(task.v2()));
                for (ActiveTask task : tasks) {
                    toCancel.add(
                        new Tuple<>(task, new IllegalStateException("runtime stopped while processing task [" + task.taskId() + "]"))
                    );
                    toRelease.add(task.lease);
                }
                tasks.clear();
                nextClaimAtMillis = Long.MAX_VALUE;
                wakeAtMillis = Long.MAX_VALUE;
            } else {
                if (claimCompleted) {
                    claimInProgress = false;
                    nextClaimAtMillis = nowMillis + claimInterval.millis();
                }
                for (Tuple<S, Lease> claimedTask : claimedTasks) {
                    final Lease lease = claimedTask.v2();
                    if (lease.expiryMillis() > nowMillis) {
                        final var task = new ActiveTask(claimedTask.v1(), lease, renewalTime(nowMillis, lease.expiryMillis()));
                        tasks.add(task);
                        toStart.add(task);
                    } else {
                        toRelease.add(lease);
                    }
                }

                for (Tuple<ActiveTask, Result<Lease, Exception>> renewal : renewals) {
                    if (renewal.v1().renewalInProgress && renewal.v1().renewalResult == null) {
                        renewal.v1().renewalResult = renewal.v2();
                    }
                }

                boolean renewalDue = false;
                for (int i = tasks.size() - 1; i >= 0; i--) {
                    final ActiveTask task = tasks.get(i);
                    if (task.isClosed()) {
                        tasks.remove(i);
                        nextClaimAtMillis = Math.min(nextClaimAtMillis, nowMillis);
                        continue;
                    }

                    if (task.renewalResult != null) {
                        final Lease renewedLease = task.renewalResult.asOptional().orElse(null);
                        final Exception renewalFailure = task.renewalResult.failure().orElse(null);
                        final boolean validRenewal = renewalFailure == null
                            && renewedLease != null
                            && renewedLease.expiryMillis() > nowMillis;
                        if (validRenewal) {
                            task.lease = renewedLease;
                            task.renewAtMillis = renewalTime(nowMillis, renewedLease.expiryMillis());
                            task.renewalInProgress = false;
                            task.renewalResult = null;
                        } else {
                            tasks.remove(i);
                            nextClaimAtMillis = Math.min(nextClaimAtMillis, nowMillis);
                            final Exception failure = renewalFailure != null
                                ? renewalFailure
                                : new IllegalStateException("queue returned an invalid renewed lease for task [" + task.taskId() + "]");
                            toCancel.add(new Tuple<>(task, failure));
                            continue;
                        }
                    }

                    if (nowMillis >= task.lease.expiryMillis()) {
                        tasks.remove(i);
                        nextClaimAtMillis = Math.min(nextClaimAtMillis, nowMillis);
                        toCancel.add(new Tuple<>(task, new IllegalStateException("lease for task [" + task.taskId() + "] has expired")));
                    } else if (task.renewalInProgress == false && nowMillis >= task.renewAtMillis) {
                        renewalDue = true;
                    }
                }

                if (renewalDue) {
                    final long renewalCutoffMillis = nowMillis + renewalBatchWindowMillis;
                    for (ActiveTask task : tasks) {
                        if (task.renewalInProgress == false && task.renewAtMillis <= renewalCutoffMillis) {
                            task.renewalInProgress = true;
                            toRenew.add(new Tuple<>(task, task.lease));
                        }
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
                    nextWakeAtMillis = Math.min(
                        nextWakeAtMillis,
                        task.renewalInProgress ? task.lease.expiryMillis() : Math.min(task.renewAtMillis, task.lease.expiryMillis())
                    );
                }
                if (nextWakeAtMillis < wakeAtMillis) {
                    wakeAtMillis = nextWakeAtMillis;
                    scheduleAtMillis = nextWakeAtMillis;
                }
            }
            taskCount = tasks.size();
        }

        for (Tuple<ActiveTask, Exception> cancellation : toCancel) {
            cancellation.v1().cancel(cancellation.v2());
        }
        toRelease.forEach(this::release);
        if (toRenew.isEmpty() == false) {
            renew(toRenew);
        }
        if (claimCapacity > 0) {
            claim(claimCapacity);
        }
        if (scheduleAtMillis != Long.MAX_VALUE) {
            scheduleWake(scheduleAtMillis);
        }
        toStart.forEach(this::startExecution);
    }

    private void claim(int capacity) {
        queue.claim(workerId, capacity, leaseDuration, new ActionListener<>() {
            @Override
            public void onResponse(List<Tuple<S, Lease>> claimedTasks) {
                reconcile(true, claimedTasks, List.of());
            }

            @Override
            public void onFailure(Exception failure) {
                logger.debug("failed to claim queued tasks", failure);
                reconcile(true, List.of(), List.of());
            }
        });
    }

    private void renew(List<Tuple<ActiveTask, Lease>> renewals) {
        final List<Lease> leases = renewals.stream().map(Tuple::v2).toList();
        final ActionListener<List<Result<Lease, Exception>>> listener = ActionListener.assertOnce(
            ActionListener.wrap(results -> renewalsCompleted(renewals, results), failure -> renewalsFailed(renewals, failure))
        );
        queue.renew(leases, leaseDuration, listener);
    }

    private void renewalsCompleted(List<Tuple<ActiveTask, Lease>> renewals, List<Result<Lease, Exception>> results) {
        if (results == null || results.size() != renewals.size() || results.stream().anyMatch(Objects::isNull)) {
            renewalsFailed(renewals, new IllegalStateException("queue returned an invalid bulk renewal response"));
            return;
        }

        final List<Tuple<ActiveTask, Result<Lease, Exception>>> completedRenewals = new ArrayList<>(renewals.size());
        for (int i = 0; i < renewals.size(); i++) {
            completedRenewals.add(new Tuple<>(renewals.get(i).v1(), results.get(i)));
        }
        reconcile(false, List.of(), completedRenewals);
    }

    private void renewalsFailed(List<Tuple<ActiveTask, Lease>> renewals, Exception failure) {
        final List<Tuple<ActiveTask, Result<Lease, Exception>>> failedRenewals = new ArrayList<>(renewals.size());
        for (Tuple<ActiveTask, Lease> renewal : renewals) {
            failedRenewals.add(new Tuple<>(renewal.v1(), Result.failure(failure)));
        }
        reconcile(false, List.of(), failedRenewals);
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
                    process(task);
                } catch (Exception e) {
                    task.processorFailed(e);
                }
            });
        } catch (Exception e) {
            task.processorFailed(e);
        }
    }

    private void scheduleWake(long atMillis) {
        final long delayMillis = Math.max(1L, atMillis - threadPool.absoluteTimeInMillis());
        try {
            threadPool.schedule(this::reconcile, TimeValue.timeValueMillis(delayMillis), threadPool.generic());
        } catch (EsRejectedExecutionException e) {
            logger.debug("stopping task processor runtime because task queue maintenance was rejected", e);
            stopProcessing();
        }
    }

    private void release(Lease lease) {
        final ActionListener<Void> listener = ActionListener.wrap(
            ignored -> {},
            failure -> logger.debug(() -> "failed to release lease for task [" + lease.taskId() + "]", failure)
        );
        queue.release(lease, listener);
    }

    private static long renewalTime(long nowMillis, long expiryMillis) {
        return nowMillis + Math.max(1L, (expiryMillis - nowMillis) / 2L);
    }

    // Exposed for tests.
    int activeTaskCount() {
        return taskCount;
    }

    private record LocalState<T>(T state, boolean closed, boolean terminalUpdate, ActionListener<T> updateListener) {}

    /** Local state for one active lease incarnation. */
    private final class ActiveTask implements TaskHandle<S> {

        private final String taskId;
        private final AtomicReference<LocalState<S>> localState;
        // Written by reconciliation under mutex and read by processor threads when starting updates.
        private volatile Lease lease;
        // Accessed only by reconciliation under mutex.
        private long renewAtMillis;
        private boolean renewalInProgress;
        private Result<Lease, Exception> renewalResult;

        private ActiveTask(S state, Lease lease, long renewAtMillis) {
            this.taskId = lease.taskId();
            this.localState = new AtomicReference<>(new LocalState<>(state, false, false, null));
            this.lease = lease;
            this.renewAtMillis = renewAtMillis;
        }

        private String taskId() {
            return taskId;
        }

        private boolean isClosed() {
            return localState.get().closed();
        }

        @Override
        public S state() {
            return localState.get().state();
        }

        @Override
        public void update(S newState, boolean terminal, ActionListener<S> listener) {
            final LocalState<S> current = localState.get();
            if (current.closed()) {
                listener.onFailure(new IllegalStateException("lease for task [" + taskId + "] is no longer active"));
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
                    ? new IllegalStateException("lease for task [" + taskId + "] is no longer active")
                    : new IllegalStateException("task [" + taskId + "] changed concurrently while starting a persistent state change");
                listener.onFailure(failure);
                return;
            }

            final ActionListener<S> operationListener = ActionListener.assertOnce(
                ActionListener.wrap(persistedState -> stateChangeCompleted(pendingUpdate, persistedState), this::stateChangeFailed)
            );
            final Lease lease = this.lease;
            if (threadPool.absoluteTimeInMillis() >= lease.expiryMillis()) {
                operationListener.onFailure(new IllegalStateException("lease for task [" + taskId + "] has expired"));
            } else {
                queue.update(lease, newState, terminal, operationListener);
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

        private void stateChangeFailed(Exception failure) {
            cancel(failure);
        }

        private void processorFailed(Exception failure) {
            logger.warn(() -> "task processor failed unexpectedly for task [" + taskId + "]", failure);
            cancel(new IllegalStateException("task processor failed for task [" + taskId + "]", failure));
        }

        private void cancel(Exception failure) {
            final LocalState<S> previous = localState.getAndUpdate(
                current -> current.closed() ? current : new LocalState<>(current.state(), true, false, null)
            );
            if (previous.closed() == false) {
                try {
                    TaskProcessorRuntime.this.cancel(this);
                } catch (Exception e) {
                    logger.warn(() -> "task processor failed to cancel task [" + taskId + "]", e);
                }
                if (previous.updateListener() != null) {
                    try {
                        previous.updateListener().onFailure(failure);
                    } catch (Exception e) {
                        logger.warn(() -> "state-change listener failed while cancelling task [" + taskId + "]", e);
                    }
                }
            }
            reconcile();
        }
    }
}
