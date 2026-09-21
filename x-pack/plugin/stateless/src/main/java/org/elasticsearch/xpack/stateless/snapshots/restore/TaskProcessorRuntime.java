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
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

    /** Quickly signals processing to stop for a task whose lease is no longer owned by this runtime. This method must not block. */
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
        TimeValue claimInterval,
        TimeValue shutdownGracePeriod
    ) {
        Objects.requireNonNull(clusterService);
        this.queue = Objects.requireNonNull(queue);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.processorExecutor = Objects.requireNonNull(processorExecutor);
        this.workerId = Objects.requireNonNull(workerId);
        Objects.requireNonNull(leaseDuration);
        Objects.requireNonNull(claimInterval);
        Objects.requireNonNull(shutdownGracePeriod);
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
        this.renewalBatchWindowMillis = Math.clamp(leaseDuration.millis() / 10L, 1L, claimInterval.millis());
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void afterStart() {
                startProcessing();
            }

            @Override
            public void beforeStop() {
                stopProcessing();
                try {
                    Thread.sleep(shutdownGracePeriod.toDuration());
                } catch (InterruptedException e) {}
            }
        });
    }

    private static final Logger logger = LogManager.getLogger(TaskProcessorRuntime.class);
    private static final long RENEWAL_IN_PROGRESS = Long.MAX_VALUE;
    private final TaskQueue<S> queue;
    private final ThreadPool threadPool;
    private final Executor processorExecutor;
    private final String workerId;
    private final int maxConcurrentTasks;
    private final TimeValue leaseDuration;
    private final TimeValue claimInterval;
    private final long renewalBatchWindowMillis;

    private final Object mutex = new Object();
    private final Map<String, ActiveTask> tasks = new HashMap<>();
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
        reconcile(false, List.of(), Map.of());
    }

    /**
     * Reconciles all runtime-owned state under one short critical section. Queue, processor and scheduler calls are dispatched afterwards.
     */
    private void reconcile(
        boolean claimCompleted,
        List<Tuple<S, Lease>> claimedTasks,
        Map<Lease, Result<Lease, Exception>> renewalResults
    ) {
        final List<Lease> toRenew = new ArrayList<>();
        final List<ActiveTask> toCancel = new ArrayList<>();
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
                for (ActiveTask task : tasks.values()) {
                    toCancel.add(task);
                    toRelease.add(task.lease);
                }
                tasks.clear();
                nextClaimAtMillis = Long.MAX_VALUE;
                wakeAtMillis = Long.MAX_VALUE;
            } else {
                if (claimCompleted) {
                    claimInProgress = false;
                    nextClaimAtMillis = Math.min(nextClaimAtMillis, nowMillis + claimInterval.millis());
                }
                for (Tuple<S, Lease> claimedTask : claimedTasks) {
                    final Lease lease = claimedTask.v2();
                    if (lease.expiryMillis() > nowMillis) {
                        final var task = new ActiveTask(claimedTask.v1(), lease, renewalTime(nowMillis, lease.expiryMillis()));
                        tasks.put(lease.taskId(), task);
                        toStart.add(task);
                    } else {
                        toRelease.add(lease);
                    }
                }

                for (Map.Entry<Lease, Result<Lease, Exception>> renewal : renewalResults.entrySet()) {
                    final Lease requestedLease = renewal.getKey();
                    final ActiveTask task = tasks.get(requestedLease.taskId());
                    if (task == null || task.lease.equals(requestedLease) == false || task.renewAtMillis != RENEWAL_IN_PROGRESS) {
                        continue;
                    }
                    final Result<Lease, Exception> result = renewal.getValue();
                    if (result.isSuccessful()) {
                        final Lease renewedLease = result.asOptional().orElseThrow();
                        task.lease = renewedLease;
                        task.renewAtMillis = renewalTime(nowMillis, renewedLease.expiryMillis());
                    } else {
                        logger.debug(
                            () -> "failed to renew lease for task [" + requestedLease.taskId() + "]",
                            result.failure().orElseThrow()
                        );
                        tasks.remove(requestedLease.taskId());
                        nextClaimAtMillis = Math.min(nextClaimAtMillis, nowMillis);
                        toCancel.add(task);
                    }
                }

                boolean renewalDue = false;
                final Iterator<ActiveTask> taskIterator = tasks.values().iterator();
                while (taskIterator.hasNext()) {
                    final ActiveTask task = taskIterator.next();
                    final LocalState<S> taskState = task.localState.get();
                    if (taskState.closed()) {
                        taskIterator.remove();
                        nextClaimAtMillis = Math.min(nextClaimAtMillis, nowMillis);
                        if (taskState.terminalUpdate() == false) {
                            toRelease.add(task.lease);
                        }
                        continue;
                    }

                    if (nowMillis >= task.lease.expiryMillis()) {
                        taskIterator.remove();
                        nextClaimAtMillis = Math.min(nextClaimAtMillis, nowMillis);
                        toCancel.add(task);
                    } else if (nowMillis >= task.renewAtMillis) {
                        renewalDue = true;
                    }
                }

                if (renewalDue) {
                    final long renewalCutoffMillis = nowMillis + renewalBatchWindowMillis;
                    for (ActiveTask task : tasks.values()) {
                        if (task.renewAtMillis <= renewalCutoffMillis) {
                            task.renewAtMillis = RENEWAL_IN_PROGRESS;
                            toRenew.add(task.lease);
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
                for (ActiveTask task : tasks.values()) {
                    nextWakeAtMillis = Math.min(nextWakeAtMillis, Math.min(task.renewAtMillis, task.lease.expiryMillis()));
                }
                if (nextWakeAtMillis < wakeAtMillis) {
                    wakeAtMillis = nextWakeAtMillis;
                    scheduleAtMillis = nextWakeAtMillis;
                }
            }
            taskCount = tasks.size();
        }

        for (ActiveTask task : toCancel) {
            task.cancel();
        }
        if (toRelease.isEmpty() == false) {
            release(toRelease);
        }
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
                reconcile(true, claimedTasks, Map.of());
            }

            @Override
            public void onFailure(Exception failure) {
                logger.debug("failed to claim queued tasks", failure);
                reconcile(true, List.of(), Map.of());
            }
        });
    }

    private void renew(List<Lease> leases) {
        queue.renew(leases, leaseDuration, new ActionListener<>() {
            @Override
            public void onResponse(Map<Lease, Result<Lease, Exception>> results) {
                reconcile(false, List.of(), results);
            }

            @Override
            public void onFailure(Exception failure) {
                final Map<Lease, Result<Lease, Exception>> results = HashMap.newHashMap(leases.size());
                for (Lease lease : leases) {
                    results.put(lease, Result.failure(failure));
                }
                reconcile(false, List.of(), results);
            }
        });
    }

    private void startExecution(ActiveTask task) {
        if (task.localState.get().closed()) {
            return;
        }
        try {
            processorExecutor.execute(() -> {
                if (task.localState.get().closed()) {
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

    private void release(List<Lease> leases) {
        final ActionListener<Void> listener = ActionListener.wrap(
            ignored -> {},
            failure -> logger.debug(() -> "failed to release [" + leases.size() + "] task leases", failure)
        );
        queue.release(leases, listener);
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

        private ActiveTask(S state, Lease lease, long renewAtMillis) {
            this.taskId = lease.taskId();
            this.localState = new AtomicReference<>(new LocalState<>(state, false, false, null));
            this.lease = lease;
            this.renewAtMillis = renewAtMillis;
        }

        private String taskId() {
            return taskId;
        }

        @Override
        public S state() {
            return localState.get().state();
        }

        @Override
        public void update(S newState, boolean terminal, ActionListener<S> listener) {
            // TODO here:
            //  - remove defensive check on concurrent update
            //  - the listener should be passed around, not put in state
            //  - can remove that compare and exchange
            //  - terminal should not be set until success
            //  - the wrap etc is pointless
            //  - the lease check can happen earlier, and should call cancel
            //  - once inlined, the two listeners can probably be simplified further, dont' have to CAS to get the update listener
            //  - have to figure out populating the closed and terminal
            //  - cancel should take no args
            //  - if update fails, don't call the listener at all, just cancel
            final LocalState<S> current = localState.get();
            if (current.closed()) {
                listener.onFailure(new InterruptedException());
                return;
            }
            if (current.updateListener() != null) {
                listener.onFailure(new ConcurrentModificationException());
                return;
            }

            final LocalState<S> pendingUpdate = new LocalState<>(current.state(), false, terminal, listener);
            final LocalState<S> witness = localState.compareAndExchange(current, pendingUpdate);
            if (witness != current) {
                listener.onFailure(witness.closed() ? new InterruptedException() : new ConcurrentModificationException());
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
            logger.debug(() -> "state change failed for task [" + taskId + "]", failure);
            cancel();
            reconcile();
        }

        private void processorFailed(Exception failure) {
            logger.warn(() -> "task processor failed unexpectedly for task [" + taskId + "]", failure);
            cancel();
            reconcile();
        }

        private void cancel() {
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
                        previous.updateListener().onFailure(new InterruptedException());
                    } catch (Exception e) {
                        logger.warn(() -> "state-change listener failed while cancelling task [" + taskId + "]", e);
                    }
                }
            }
        }
    }
}
