/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.restore;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.util.Result;
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
    private final long renewalBatchWindowMillis;

    private final Object mutex = new Object();
    private final List<ActiveTask> tasks = new ArrayList<>();
    private volatile boolean running;
    private boolean claimInProgress;
    private long nextClaimAtMillis;
    private long wakeAtMillis = Long.MAX_VALUE;
    private volatile int taskCount;

    private record LocalState<T>(T state, boolean closed, boolean terminalUpdate, ActionListener<T> updateListener) {}

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
        this.renewalBatchWindowMillis = Math.min(claimInterval.millis(), Math.max(1L, leaseDuration.millis() / 10L));
    }

    @Override
    protected void doStart() {
        running = true;
        reconcile(null, null, null);
    }

    @Override
    protected void doStop() {
        running = false;
        reconcile(null, null, null);
    }

    @Override
    protected void doClose() {}

    /**
     * Reconciles all runtime-owned state under one short critical section. Queue, processor and scheduler calls are dispatched afterwards.
     */
    private void reconcile(List<Tuple<S, Lease>> claimedTasks, RenewalBatch renewalBatch, Exception schedulingFailure) {
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

            if (claimedTasks != null) {
                claimInProgress = false;
                if (running && schedulingFailure == null) {
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

            if (renewalBatch != null && running && schedulingFailure == null) {
                for (int i = 0; i < renewalBatch.renewals.size(); i++) {
                    final Tuple<ActiveTask, Lease> renewal = renewalBatch.renewals.get(i);
                    final ActiveTask task = renewal.v1();
                    if (task.lease == renewal.v2() && task.renewalInProgress && task.renewalResult == null) {
                        task.renewalResult = renewalBatch.results.get(i);
                    }
                }
            }

            if (running == false || schedulingFailure != null) {
                for (ActiveTask task : tasks) {
                    final Exception failure = schedulingFailure == null
                        ? new LeaseLostException("runtime stopped while processing task [" + task.taskId() + "]")
                        : new LeaseLostException("could not schedule task queue maintenance", schedulingFailure);
                    toCancel.add(new Tuple<>(task, failure));
                    if (schedulingFailure == null) {
                        toRelease.add(task.lease);
                    }
                }
                tasks.clear();
                nextClaimAtMillis = Long.MAX_VALUE;
                wakeAtMillis = Long.MAX_VALUE;
            } else {
                boolean renewalDue = false;
                for (int i = tasks.size() - 1; i >= 0; i--) {
                    final ActiveTask task = tasks.get(i);
                    if (task.isClosed()) {
                        tasks.remove(i);
                        nextClaimAtMillis = Math.min(nextClaimAtMillis, nowMillis);
                        continue;
                    }

                    if (task.renewalResult != null) {
                        final Long renewedExpiryMillis = task.renewalResult.asOptional().orElse(null);
                        final Exception renewalFailure = task.renewalResult.failure().orElse(null);
                        final boolean validRenewal = renewalFailure == null
                            && renewedExpiryMillis != null
                            && renewedExpiryMillis > nowMillis;
                        if (validRenewal) {
                            final Lease renewedLease = new Lease(
                                task.lease.taskId(),
                                task.lease.ownerId(),
                                task.lease.fencingToken(),
                                renewedExpiryMillis
                            );
                            task.lease = renewedLease;
                            task.renewAtMillis = renewalTime(nowMillis, renewedExpiryMillis);
                            task.renewalInProgress = false;
                            task.renewalRetryPending = false;
                            task.renewalResult = null;
                        } else if (renewalFailure != null
                            && renewalFailure instanceof LeaseLostException == false
                            && nowMillis < task.lease.expiryMillis()) {
                                final long normalRetryMillis = Math.max(1L, leaseDuration.millis() / 10L);
                                final long remainingMillis = task.lease.expiryMillis() - nowMillis;
                                task.renewAtMillis = nowMillis + Math.clamp(remainingMillis / 2L, 1L, normalRetryMillis);
                                task.renewalInProgress = false;
                                task.renewalRetryPending = true;
                                task.renewalResult = null;
                            } else {
                                tasks.remove(i);
                                nextClaimAtMillis = Math.min(nextClaimAtMillis, nowMillis);
                                final Exception failure;
                                if (renewalFailure instanceof LeaseLostException) {
                                    failure = renewalFailure;
                                } else if (renewalFailure != null) {
                                    failure = new LeaseLostException("lease for task [" + task.taskId() + "] has expired", renewalFailure);
                                } else {
                                    failure = new LeaseLostException(
                                        "queue returned an invalid renewed lease for task [" + task.taskId() + "]"
                                    );
                                }
                                toCancel.add(new Tuple<>(task, failure));
                                continue;
                            }
                    }

                    if (nowMillis >= task.lease.expiryMillis()) {
                        tasks.remove(i);
                        nextClaimAtMillis = Math.min(nextClaimAtMillis, nowMillis);
                        toCancel.add(new Tuple<>(task, new LeaseLostException("lease for task [" + task.taskId() + "] has expired")));
                    } else if (task.renewalInProgress == false && nowMillis >= task.renewAtMillis) {
                        renewalDue = true;
                    }
                }

                if (renewalDue) {
                    final long renewalCutoffMillis = nowMillis + renewalBatchWindowMillis;
                    for (ActiveTask task : tasks) {
                        if (task.renewalInProgress == false
                            && (task.renewAtMillis <= nowMillis
                                || task.renewalRetryPending == false && task.renewAtMillis <= renewalCutoffMillis)) {
                            task.renewalInProgress = true;
                            task.renewalRetryPending = false;
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
            cancellation.v1().leaseLost(cancellation.v2());
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
        final ActionListener<List<Tuple<S, Lease>>> listener = ActionListener.assertOnce(
            ActionListener.wrap(this::claimsCompleted, this::claimFailed)
        );
        queue.claim(workerId, capacity, leaseDuration, listener);
    }

    private void claimsCompleted(List<Tuple<S, Lease>> claimedTasks) {
        reconcile(claimedTasks, null, null);
    }

    private void claimFailed(Exception failure) {
        logger.debug("failed to claim queued tasks", failure);
        reconcile(List.of(), null, null);
    }

    private void renew(List<Tuple<ActiveTask, Lease>> renewals) {
        final List<Lease> leases = renewals.stream().map(Tuple::v2).toList();
        final ActionListener<List<Result<Long, Exception>>> listener = ActionListener.assertOnce(
            ActionListener.wrap(results -> renewalsCompleted(renewals, results), failure -> renewalsFailed(renewals, failure))
        );
        queue.renew(leases, leaseDuration, listener);
    }

    private void renewalsCompleted(List<Tuple<ActiveTask, Lease>> renewals, List<Result<Long, Exception>> results) {
        if (results == null || results.size() != renewals.size() || results.stream().anyMatch(Objects::isNull)) {
            renewalsFailed(renewals, new IllegalStateException("queue returned an invalid bulk renewal response"));
            return;
        }

        reconcile(null, new RenewalBatch(renewals, results), null);
    }

    private void renewalsFailed(List<Tuple<ActiveTask, Lease>> renewals, Exception failure) {
        reconcile(
            null,
            new RenewalBatch(renewals, renewals.stream().map(ignored -> Result.<Long, Exception>failure(failure)).toList()),
            null
        );
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

    private void scheduleWake(long atMillis) {
        final long delayMillis = Math.max(1L, atMillis - threadPool.absoluteTimeInMillis());
        try {
            threadPool.schedule(() -> reconcile(null, null, null), TimeValue.timeValueMillis(delayMillis), threadPool.generic());
        } catch (Exception e) {
            reconcile(null, null, e);
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
        queue.release(lease, listener);
    }

    private static long renewalTime(long nowMillis, long expiryMillis) {
        return nowMillis + Math.max(1L, (expiryMillis - nowMillis) / 2L);
    }

    // Exposed for tests.
    int activeTaskCount() {
        return taskCount;
    }

    private final class RenewalBatch {

        private final List<Tuple<ActiveTask, Lease>> renewals;
        private final List<Result<Long, Exception>> results;

        private RenewalBatch(List<Tuple<ActiveTask, Lease>> renewals, List<Result<Long, Exception>> results) {
            this.renewals = renewals;
            this.results = results;
        }
    }

    /** Local state for one active lease incarnation. */
    private final class ActiveTask implements TaskHandle<S> {

        private final String taskId;
        private final AtomicReference<LocalState<S>> localState;
        // Written by reconciliation under mutex and read by processor threads when starting updates.
        private volatile Lease lease;
        // Accessed only by reconciliation under mutex.
        private long renewAtMillis;
        private boolean renewalInProgress;
        private boolean renewalRetryPending;
        private Result<Long, Exception> renewalResult;

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
            final Lease lease = this.lease;
            if (threadPool.absoluteTimeInMillis() >= lease.expiryMillis()) {
                operationListener.onFailure(new LeaseLostException("lease for task [" + taskId + "] has expired"));
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
                reconcile(null, null, null);
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
            reconcile(null, null, null);
        }

        private LocalState<S> close() {
            final LocalState<S> previous = localState.getAndUpdate(
                current -> current.closed() ? current : new LocalState<>(current.state(), true, false, null)
            );
            return previous.closed() ? null : previous;
        }
    }
}
