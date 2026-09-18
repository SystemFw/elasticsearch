/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.snapshots.TaskProcessor.TaskExecution;
import org.elasticsearch.xpack.stateless.snapshots.TaskQueue.Lease;
import org.elasticsearch.xpack.stateless.snapshots.TaskQueue.LeaseLostException;
import org.elasticsearch.xpack.stateless.snapshots.TaskQueue.LeasedTask;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/** Implements the processor-facing operations for one lease incarnation. */
final class TaskExecutionImpl<S> implements TaskExecution<S> {

    private static final Logger logger = LogManager.getLogger(TaskExecutionImpl.class);

    enum Status {
        ACTIVE,
        MODIFYING,
        FINISHING,
        RELEASING,
        LEASE_LOST,
        FINISHED,
        RELEASED
    }

    private final TaskQueue<S> queue;
    private final ThreadPool threadPool;
    private final TimeValue leaseDuration;
    private final Consumer<TaskExecutionImpl<S>> onEnded;
    private final List<Runnable> leaseLostListeners = new ArrayList<>();
    private final String taskId;

    // The following fields are guarded by this.
    private Lease lease;
    private S state;
    private Status status = Status.ACTIVE;
    private ActionListener<S> pendingStateListener;
    private Scheduler.Cancellable renewalTask;
    private Scheduler.Cancellable expiryTask;
    private boolean renewalInProgress;

    TaskExecutionImpl(
        LeasedTask<S> task,
        TaskQueue<S> queue,
        ThreadPool threadPool,
        TimeValue leaseDuration,
        Consumer<TaskExecutionImpl<S>> onEnded
    ) {
        this.taskId = task.id();
        this.lease = task.lease();
        this.state = task.state();
        this.queue = Objects.requireNonNull(queue);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.leaseDuration = Objects.requireNonNull(leaseDuration);
        this.onEnded = Objects.requireNonNull(onEnded);
    }

    String taskId() {
        return taskId;
    }

    synchronized Status status() {
        return status;
    }

    synchronized boolean canProcess() {
        return isLive(status);
    }

    boolean start() {
        scheduleLeaseChecks();
        synchronized (this) {
            return isLive(status);
        }
    }

    @Override
    public synchronized S state() {
        return state;
    }

    @Override
    public void modify(S newState, ActionListener<S> listener) {
        changeState(newState, Status.MODIFYING, listener);
    }

    @Override
    public void finish(S finalState, ActionListener<S> listener) {
        changeState(finalState, Status.FINISHING, listener);
    }

    private void changeState(S newState, Status operation, ActionListener<S> listener) {
        Objects.requireNonNull(newState);
        Objects.requireNonNull(listener);

        final Lease currentLease;
        final Exception rejection;
        synchronized (this) {
            if (status == Status.ACTIVE) {
                status = operation;
                pendingStateListener = listener;
                currentLease = lease;
                rejection = null;
            } else {
                currentLease = null;
                rejection = operationRejection();
            }
        }

        if (rejection != null) {
            listener.onFailure(rejection);
            return;
        }

        final ActionListener<S> operationListener = ActionListener.assertOnce(
            ActionListener.wrap(persistedState -> stateChangeCompleted(operation, persistedState), this::stateChangeFailed)
        );
        try {
            if (operation == Status.MODIFYING) {
                queue.modify(currentLease, newState, operationListener);
            } else {
                queue.finish(currentLease, newState, operationListener);
            }
        } catch (Exception e) {
            operationListener.onFailure(e);
        }
    }

    private Exception operationRejection() {
        return switch (status) {
            case RELEASING, LEASE_LOST, RELEASED -> new LeaseLostException("lease for task [" + lease.taskId() + "] is no longer active");
            case FINISHED -> new IllegalStateException("task [" + lease.taskId() + "] is already finished");
            case MODIFYING, FINISHING -> new IllegalStateException(
                "task [" + lease.taskId() + "] already has a persistent state change in progress"
            );
            case ACTIVE -> throw new AssertionError("active execution must accept the operation");
        };
    }

    private void stateChangeCompleted(Status operation, S persistedState) {
        final ActionListener<S> listener;
        final boolean finished;
        synchronized (this) {
            if (status != operation) {
                return;
            }

            state = Objects.requireNonNull(persistedState);
            listener = pendingStateListener;
            pendingStateListener = null;
            finished = operation == Status.FINISHING;
            status = finished ? Status.FINISHED : Status.ACTIVE;
            if (finished) {
                cancelLeaseTasks();
            }
        }

        if (finished) {
            onEnded.accept(this);
        }
        listener.onResponse(persistedState);
    }

    private void stateChangeFailed(Exception failure) {
        if (failure instanceof LeaseLostException) {
            loseLease(failure);
            return;
        }

        final ActionListener<S> listener;
        synchronized (this) {
            if (status != Status.MODIFYING && status != Status.FINISHING) {
                return;
            }
            status = Status.ACTIVE;
            listener = pendingStateListener;
            pendingStateListener = null;
        }
        listener.onFailure(failure);
    }

    @Override
    public void addLeaseLostListener(Runnable listener) {
        Objects.requireNonNull(listener);
        final boolean notifyNow;
        synchronized (this) {
            notifyNow = status == Status.RELEASING || status == Status.LEASE_LOST || status == Status.RELEASED;
            if (notifyNow == false && status != Status.FINISHED) {
                leaseLostListeners.add(listener);
            }
        }
        if (notifyNow) {
            runLeaseLostListener(listener);
        }
    }

    void processorFailed(Exception failure) {
        logger.warn(() -> "task processor failed unexpectedly for task [" + taskId() + "]", failure);
        loseLease(new LeaseLostException("task processor failed for task [" + taskId() + "]", failure));
    }

    void runtimeStopped() {
        final Lease leaseToRelease;
        final ActionListener<S> stateListener;
        final List<Runnable> listeners;
        synchronized (this) {
            if (isTerminal(status)) {
                return;
            }
            status = Status.RELEASING;
            cancelLeaseTasks();
            stateListener = pendingStateListener;
            pendingStateListener = null;
            listeners = takeLeaseLostListeners();
            leaseToRelease = lease;
        }

        final var failure = new LeaseLostException("runtime stopped while processing task [" + taskId() + "]");
        if (stateListener != null) {
            notifyStateFailure(stateListener, failure);
        }
        listeners.forEach(this::runLeaseLostListener);

        final ActionListener<Void> releaseListener = ActionListener.assertOnce(
            ActionListener.wrap(ignored -> releaseCompleted(null), this::releaseCompleted)
        );
        try {
            queue.release(leaseToRelease, releaseListener);
        } catch (Exception e) {
            releaseListener.onFailure(e);
        }
    }

    private void releaseCompleted(Exception failure) {
        synchronized (this) {
            if (status != Status.RELEASING) {
                return;
            }
            status = Status.RELEASED;
        }
        if (failure != null) {
            logger.debug(() -> "failed to release lease for task [" + taskId() + "]; it will remain unavailable until expiry", failure);
        }
        onEnded.accept(this);
    }

    private void scheduleLeaseChecks() {
        Exception schedulingFailure = null;
        synchronized (this) {
            if (isLive(status) == false) {
                return;
            }
            cancelLeaseTasks();
            final long remainingMillis = lease.expiryMillis() - threadPool.absoluteTimeInMillis();
            if (remainingMillis <= 0L) {
                schedulingFailure = new LeaseLostException("lease for task [" + taskId() + "] has expired");
            } else {
                try {
                    renewalTask = threadPool.schedule(this::renewLease, renewalDelay(remainingMillis), threadPool.generic());
                    final long expectedExpiryMillis = lease.expiryMillis();
                    expiryTask = threadPool.schedule(
                        () -> expireLease(expectedExpiryMillis),
                        TimeValue.timeValueMillis(remainingMillis),
                        threadPool.generic()
                    );
                } catch (Exception e) {
                    cancelLeaseTasks();
                    schedulingFailure = e;
                }
            }
        }
        if (schedulingFailure != null) {
            loseLease(new LeaseLostException("could not schedule lease renewal for task [" + taskId() + "]", schedulingFailure));
        }
    }

    private static TimeValue renewalDelay(long remainingMillis) {
        return TimeValue.timeValueMillis(Math.max(1L, remainingMillis / 2L));
    }

    private void renewLease() {
        final Lease leaseToRenew;
        synchronized (this) {
            if (isLive(status) == false || renewalInProgress) {
                return;
            }
            if (threadPool.absoluteTimeInMillis() >= lease.expiryMillis()) {
                leaseToRenew = null;
            } else {
                renewalInProgress = true;
                leaseToRenew = lease;
            }
        }

        if (leaseToRenew == null) {
            loseLease(new LeaseLostException("lease for task [" + taskId() + "] has expired"));
            return;
        }

        final ActionListener<Lease> listener = ActionListener.assertOnce(ActionListener.wrap(this::leaseRenewed, this::leaseRenewalFailed));
        try {
            queue.renew(leaseToRenew, leaseDuration, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void leaseRenewed(Lease renewedLease) {
        final Exception invalidLease;
        synchronized (this) {
            renewalInProgress = false;
            if (isLive(status) == false) {
                return;
            }
            if (renewedLease == null
                || sameLeaseIncarnation(lease, renewedLease) == false
                || renewedLease.expiryMillis() <= threadPool.absoluteTimeInMillis()) {
                invalidLease = new LeaseLostException("queue returned an invalid renewed lease for task [" + taskId() + "]");
            } else {
                lease = renewedLease;
                invalidLease = null;
            }
        }
        if (invalidLease == null) {
            scheduleLeaseChecks();
        } else {
            loseLease(invalidLease);
        }
    }

    private static boolean sameLeaseIncarnation(Lease current, Lease renewed) {
        return current.taskId().equals(renewed.taskId())
            && current.ownerId().equals(renewed.ownerId())
            && current.fencingToken() == renewed.fencingToken();
    }

    private void leaseRenewalFailed(Exception failure) {
        final boolean finishing;
        synchronized (this) {
            renewalInProgress = false;
            if (isLive(status) == false) {
                return;
            }
            finishing = status == Status.FINISHING;
        }
        if (failure instanceof LeaseLostException) {
            // The finish may already be durable, in which case renewal observes a terminal task before the finish response reaches us.
            // Let the fenced finish operation decide the outcome. The existing expiry watchdog remains active if its response is delayed.
            if (finishing == false) {
                loseLease(failure);
            }
        } else {
            scheduleRenewalRetry();
        }
    }

    private void scheduleRenewalRetry() {
        Exception schedulingFailure = null;
        synchronized (this) {
            if (isLive(status) == false) {
                return;
            }
            final long remainingMillis = lease.expiryMillis() - threadPool.absoluteTimeInMillis();
            if (remainingMillis <= 0L) {
                schedulingFailure = new LeaseLostException("lease for task [" + taskId() + "] has expired");
            } else {
                final long normalRetryMillis = Math.max(1L, leaseDuration.millis() / 10L);
                final long retryMillis = Math.min(normalRetryMillis, Math.max(1L, remainingMillis / 2L));
                try {
                    renewalTask = threadPool.schedule(this::renewLease, TimeValue.timeValueMillis(retryMillis), threadPool.generic());
                } catch (Exception e) {
                    schedulingFailure = e;
                }
            }
        }
        if (schedulingFailure != null) {
            loseLease(new LeaseLostException("could not retry lease renewal for task [" + taskId() + "]", schedulingFailure));
        }
    }

    private void expireLease(long expectedExpiryMillis) {
        final boolean expired;
        synchronized (this) {
            expired = isLive(status)
                && lease.expiryMillis() == expectedExpiryMillis
                && threadPool.absoluteTimeInMillis() >= expectedExpiryMillis;
        }
        if (expired) {
            loseLease(new LeaseLostException("lease for task [" + taskId() + "] has expired"));
        }
    }

    private void loseLease(Exception failure) {
        final ActionListener<S> stateListener;
        final List<Runnable> listeners;
        synchronized (this) {
            if (isTerminal(status)) {
                return;
            }
            status = Status.LEASE_LOST;
            cancelLeaseTasks();
            stateListener = pendingStateListener;
            pendingStateListener = null;
            listeners = takeLeaseLostListeners();
        }

        onEnded.accept(this);
        if (stateListener != null) {
            notifyStateFailure(stateListener, failure);
        }
        listeners.forEach(this::runLeaseLostListener);
    }

    private void notifyStateFailure(ActionListener<S> listener, Exception failure) {
        try {
            listener.onFailure(failure);
        } catch (Exception e) {
            logger.warn(() -> "state-change listener failed while handling lease loss for task [" + taskId() + "]", e);
        }
    }

    private List<Runnable> takeLeaseLostListeners() {
        final List<Runnable> listeners = List.copyOf(leaseLostListeners);
        leaseLostListeners.clear();
        return listeners;
    }

    private void runLeaseLostListener(Runnable listener) {
        try {
            listener.run();
        } catch (Exception e) {
            logger.warn(() -> "lease-lost listener failed for task [" + taskId() + "]", e);
        }
    }

    private void cancelLeaseTasks() {
        if (renewalTask != null) {
            renewalTask.cancel();
            renewalTask = null;
        }
        if (expiryTask != null) {
            expiryTask.cancel();
            expiryTask = null;
        }
    }

    private static boolean isLive(Status status) {
        return status == Status.ACTIVE || status == Status.MODIFYING || status == Status.FINISHING;
    }

    private static boolean isTerminal(Status status) {
        return status == Status.RELEASING || status == Status.LEASE_LOST || status == Status.FINISHED || status == Status.RELEASED;
    }
}
