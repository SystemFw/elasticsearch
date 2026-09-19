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
    private final List<LeasedTask> tasks = new ArrayList<>();
    private boolean running;
    private boolean claimInProgress;
    private long nextClaimAtMillis = Long.MAX_VALUE;
    private long wakeAtMillis = Long.MAX_VALUE;
    private long wakeGeneration;

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
        final List<LeasedTask> stoppedTasks;
        synchronized (mutex) {
            running = false;
            nextClaimAtMillis = Long.MAX_VALUE;
            wakeAtMillis = Long.MAX_VALUE;
            wakeGeneration++;
            stoppedTasks = List.copyOf(tasks);
            tasks.clear();
        }

        for (LeasedTask task : stoppedTasks) {
            final var failure = new LeaseLostException("runtime stopped while processing task [" + task.task.taskId() + "]");
            closeAndCancel(task.task, failure);
            release(task.lease);
        }
    }

    @Override
    protected void doClose() {}

    /**
     * Reconciles all runtime-owned state under one short critical section. Queue, processor and scheduler calls are dispatched afterwards.
     */
    private void reconcile() {
        final List<Tuple<LeasedTask, Lease>> toRenew = new ArrayList<>();
        final List<Tuple<TaskHandleImpl, Exception>> toCancel = new ArrayList<>();
        int claimCapacity = 0;
        long scheduleAtMillis = Long.MAX_VALUE;
        long scheduleGeneration = 0L;
        final long nowMillis = threadPool.absoluteTimeInMillis();

        synchronized (mutex) {
            if (running == false) {
                return;
            }

            for (int i = tasks.size() - 1; i >= 0; i--) {
                final LeasedTask leasedTask = tasks.get(i);
                if (leasedTask.task.isClosed()) {
                    tasks.remove(i);
                    nextClaimAtMillis = Math.min(nextClaimAtMillis, nowMillis);
                } else if (nowMillis >= leasedTask.lease.expiryMillis()) {
                    tasks.remove(i);
                    nextClaimAtMillis = Math.min(nextClaimAtMillis, nowMillis);
                    toCancel.add(
                        new Tuple<>(
                            leasedTask.task,
                            new LeaseLostException("lease for task [" + leasedTask.task.taskId() + "] has expired")
                        )
                    );
                } else if (leasedTask.renewalInProgress == false && nowMillis >= leasedTask.renewAtMillis) {
                    leasedTask.renewalInProgress = true;
                    toRenew.add(new Tuple<>(leasedTask, leasedTask.lease));
                }
            }

            final int capacity = maxConcurrentTasks - tasks.size();
            if (capacity > 0 && claimInProgress == false && nowMillis >= nextClaimAtMillis) {
                claimInProgress = true;
                nextClaimAtMillis = Long.MAX_VALUE;
                claimCapacity = capacity;
            }

            long nextWakeAtMillis = capacity > 0 && claimInProgress == false ? nextClaimAtMillis : Long.MAX_VALUE;
            for (LeasedTask leasedTask : tasks) {
                nextWakeAtMillis = Math.min(
                    nextWakeAtMillis,
                    leasedTask.renewalInProgress
                        ? leasedTask.lease.expiryMillis()
                        : Math.min(leasedTask.renewAtMillis, leasedTask.lease.expiryMillis())
                );
            }
            if (nextWakeAtMillis < wakeAtMillis) {
                wakeAtMillis = nextWakeAtMillis;
                scheduleAtMillis = nextWakeAtMillis;
                scheduleGeneration = ++wakeGeneration;
            }
        }

        for (Tuple<TaskHandleImpl, Exception> cancellation : toCancel) {
            closeAndCancel(cancellation.v1(), cancellation.v2());
        }
        for (Tuple<LeasedTask, Lease> renewal : toRenew) {
            renew(renewal.v1(), renewal.v2());
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
        final List<LeasedTask> toStart = new ArrayList<>();
        final List<Lease> toRelease = new ArrayList<>();
        final long nowMillis = threadPool.absoluteTimeInMillis();

        synchronized (mutex) {
            claimInProgress = false;
            if (running) {
                for (Tuple<S, Lease> claimedTask : claimedTasks) {
                    final Lease lease = claimedTask.v2();
                    if (tasks.size() < maxConcurrentTasks
                        && lease.expiryMillis() > nowMillis
                        && tasks.stream().noneMatch(task -> task.task.taskId().equals(lease.taskId()))) {
                        final var leasedTask = new LeasedTask(
                            new TaskHandleImpl(lease.taskId(), claimedTask.v1()),
                            lease,
                            renewalTime(nowMillis, lease.expiryMillis())
                        );
                        tasks.add(leasedTask);
                        toStart.add(leasedTask);
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
        toStart.forEach(task -> startExecution(task.task));
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

    private void startExecution(TaskHandleImpl task) {
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

    private void renew(LeasedTask task, Lease lease) {
        synchronized (mutex) {
            if (running == false || tasks.contains(task) == false || task.lease != lease || task.renewalInProgress == false) {
                return;
            }
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

    private void leaseRenewed(LeasedTask task, Lease previousLease, Lease renewedLease) {
        final long nowMillis = threadPool.absoluteTimeInMillis();
        final boolean validLease = renewedLease != null
            && sameLeaseIncarnation(previousLease, renewedLease)
            && renewedLease.expiryMillis() > nowMillis;
        boolean handled = false;
        synchronized (mutex) {
            if (running && tasks.contains(task) && task.lease == previousLease && task.renewalInProgress) {
                handled = true;
                task.renewalInProgress = false;
                if (validLease) {
                    task.lease = renewedLease;
                    task.renewAtMillis = renewalTime(nowMillis, renewedLease.expiryMillis());
                }
            }
        }

        if (handled && validLease == false) {
            closeAndCancel(
                task.task,
                new LeaseLostException("queue returned an invalid renewed lease for task [" + task.task.taskId() + "]")
            );
        }
        reconcile();
    }

    private void leaseRenewalFailed(LeasedTask task, Lease previousLease, Exception failure) {
        final long nowMillis = threadPool.absoluteTimeInMillis();
        boolean leaseLost = false;
        synchronized (mutex) {
            if (running && tasks.contains(task) && task.lease == previousLease && task.renewalInProgress) {
                task.renewalInProgress = false;
                leaseLost = failure instanceof LeaseLostException || nowMillis >= previousLease.expiryMillis();
                if (leaseLost == false) {
                    final long normalRetryMillis = Math.max(1L, leaseDuration.millis() / 10L);
                    final long remainingMillis = previousLease.expiryMillis() - nowMillis;
                    task.renewAtMillis = nowMillis + Math.clamp(remainingMillis / 2L, 1L, normalRetryMillis);
                }
            }
        }

        if (leaseLost) {
            final Exception leaseFailure = failure instanceof LeaseLostException
                ? failure
                : new LeaseLostException("lease for task [" + task.task.taskId() + "] has expired", failure);
            closeAndCancel(task.task, leaseFailure);
        }
        reconcile();
    }

    private void scheduleWake(long atMillis, long generation) {
        final long delayMillis = Math.max(1L, atMillis - threadPool.absoluteTimeInMillis());
        try {
            threadPool.schedule(() -> wake(generation), TimeValue.timeValueMillis(delayMillis), threadPool.generic());
        } catch (Exception e) {
            final List<TaskHandleImpl> affectedTasks = new ArrayList<>();
            synchronized (mutex) {
                if (running && wakeGeneration == generation) {
                    wakeAtMillis = Long.MAX_VALUE;
                    nextClaimAtMillis = Long.MAX_VALUE;
                    for (LeasedTask task : tasks) {
                        affectedTasks.add(task.task);
                    }
                    tasks.clear();
                }
            }
            final var failure = new LeaseLostException("could not schedule task queue maintenance", e);
            affectedTasks.forEach(task -> closeAndCancel(task, failure));
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

    private void update(TaskHandleImpl handle, S newState, boolean terminal, ActionListener<S> listener) {
        Lease lease = null;
        synchronized (mutex) {
            if (running) {
                for (LeasedTask task : tasks) {
                    if (task.task == handle && threadPool.absoluteTimeInMillis() < task.lease.expiryMillis()) {
                        lease = task.lease;
                        break;
                    }
                }
            }
        }
        if (lease == null) {
            listener.onFailure(new LeaseLostException("lease for task [" + handle.taskId() + "] is no longer active"));
            return;
        }
        try {
            queue.update(lease, newState, terminal, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void closeAndCancel(TaskHandleImpl task, Exception failure) {
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

    /** Runtime-owned lease and scheduling state. All fields are guarded by {@link #mutex}. */
    private final class LeasedTask {

        private final TaskHandleImpl task;
        private Lease lease;
        private long renewAtMillis;
        private boolean renewalInProgress;

        private LeasedTask(TaskHandleImpl task, Lease lease, long renewAtMillis) {
            this.task = task;
            this.lease = lease;
            this.renewAtMillis = renewAtMillis;
        }
    }

    /** Processor-facing state for one lease incarnation. */
    private final class TaskHandleImpl implements TaskHandle<S> {

        private final String taskId;
        private final AtomicReference<LocalState<S>> localState;

        private TaskHandleImpl(String taskId, S state) {
            this.taskId = taskId;
            this.localState = new AtomicReference<>(new LocalState<>(state, false, false, null));
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
            TaskProcessorRuntime.this.update(this, newState, terminal, operationListener);
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
                closeAndCancel(this, failure);
                reconcile();
                return;
            }

            final LocalState<S> failed = new LocalState<>(pendingUpdate.state(), false, false, null);
            if (localState.compareAndSet(pendingUpdate, failed)) {
                pendingUpdate.updateListener().onFailure(failure);
            }
        }

        private void processorFailed(Exception failure) {
            logger.warn(() -> "task processor failed unexpectedly for task [" + taskId + "]", failure);
            closeAndCancel(this, new LeaseLostException("task processor failed for task [" + taskId + "]", failure));
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
