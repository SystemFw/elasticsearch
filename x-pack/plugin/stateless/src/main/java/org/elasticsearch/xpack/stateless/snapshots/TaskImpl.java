/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.stateless.snapshots.TaskQueue.LeaseLostException;
import org.elasticsearch.xpack.stateless.snapshots.TaskQueue.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/** Implements the processor-facing state machine for one lease incarnation. */
final class TaskImpl<S> implements Task<S> {

    private static final Logger logger = LogManager.getLogger(TaskImpl.class);

    interface Operations<S> {
        void modify(TaskImpl<S> task, S newState, ActionListener<S> listener);

        void finish(TaskImpl<S> task, S finalState, ActionListener<S> listener);
    }

    private enum Status {
        ACTIVE,
        MODIFYING,
        FINISHING,
        RELEASING,
        LEASE_LOST,
        FINISHED,
        RELEASED
    }

    private final String taskId;
    private final Operations<S> operations;
    private final Consumer<TaskImpl<S>> onEnded;
    private final List<Runnable> leaseLostListeners = new ArrayList<>();

    // The following fields are guarded by this.
    private S state;
    private Status status = Status.ACTIVE;
    private ActionListener<S> pendingStateListener;

    TaskImpl(String taskId, S state, Operations<S> operations, Consumer<TaskImpl<S>> onEnded) {
        this.taskId = Objects.requireNonNull(taskId);
        this.state = Objects.requireNonNull(state);
        this.operations = Objects.requireNonNull(operations);
        this.onEnded = Objects.requireNonNull(onEnded);
    }

    String taskId() {
        return taskId;
    }

    synchronized boolean canProcess() {
        return isLive(status);
    }

    synchronized boolean isFinishing() {
        return status == Status.FINISHING;
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

        final Exception rejection;
        synchronized (this) {
            if (status == Status.ACTIVE) {
                status = operation;
                pendingStateListener = listener;
                rejection = null;
            } else {
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
                operations.modify(this, newState, operationListener);
            } else {
                operations.finish(this, newState, operationListener);
            }
        } catch (Exception e) {
            operationListener.onFailure(e);
        }
    }

    private Exception operationRejection() {
        return switch (status) {
            case RELEASING, LEASE_LOST, RELEASED -> new LeaseLostException("lease for task [" + taskId + "] is no longer active");
            case FINISHED -> new IllegalStateException("task [" + taskId + "] is already finished");
            case MODIFYING, FINISHING -> new IllegalStateException(
                "task [" + taskId + "] already has a persistent state change in progress"
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
        }

        if (finished) {
            onEnded.accept(this);
        }
        listener.onResponse(persistedState);
    }

    private void stateChangeFailed(Exception failure) {
        if (failure instanceof LeaseLostException) {
            leaseLost(failure);
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
        logger.warn(() -> "task processor failed unexpectedly for task [" + taskId + "]", failure);
        leaseLost(new LeaseLostException("task processor failed for task [" + taskId + "]", failure));
    }

    boolean runtimeStopped() {
        final ActionListener<S> stateListener;
        final List<Runnable> listeners;
        synchronized (this) {
            if (isTerminal(status)) {
                return false;
            }
            status = Status.RELEASING;
            stateListener = pendingStateListener;
            pendingStateListener = null;
            listeners = takeLeaseLostListeners();
        }

        final var failure = new LeaseLostException("runtime stopped while processing task [" + taskId + "]");
        if (stateListener != null) {
            notifyStateFailure(stateListener, failure);
        }
        listeners.forEach(this::runLeaseLostListener);
        return true;
    }

    void releaseCompleted(Exception failure) {
        synchronized (this) {
            if (status != Status.RELEASING) {
                return;
            }
            status = Status.RELEASED;
        }
        if (failure != null) {
            logger.debug(() -> "failed to release lease for task [" + taskId + "]", failure);
        }
        onEnded.accept(this);
    }

    void leaseLost(Exception failure) {
        final ActionListener<S> stateListener;
        final List<Runnable> listeners;
        synchronized (this) {
            if (isTerminal(status)) {
                return;
            }
            status = Status.LEASE_LOST;
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
            logger.warn(() -> "state-change listener failed while handling lease loss for task [" + taskId + "]", e);
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
            logger.warn(() -> "lease-lost listener failed for task [" + taskId + "]", e);
        }
    }

    private static boolean isLive(Status status) {
        return status == Status.ACTIVE || status == Status.MODIFYING || status == Status.FINISHING;
    }

    private static boolean isTerminal(Status status) {
        return status == Status.RELEASING || status == Status.LEASE_LOST || status == Status.FINISHED || status == Status.RELEASED;
    }
}
