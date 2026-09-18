/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;

import java.util.List;
import java.util.Objects;

/**
 * Durable queue operations needed by {@link TaskProcessorRuntime}. Implementations must validate the lease identity for every operation
 * other than {@link #claim}. Renewal and state changes must additionally require the task to be running; release may also recognize an
 * already-released lease to provide idempotence.
 */
public interface TaskQueue<S> {

    /**
     * Identifies one ownership incarnation of a queued task.
     *
     * @param taskId the durable task identifier
     * @param ownerId the worker that owns the lease
     * @param fencingToken the generation checked by every operation under the lease
     * @param expiryMillis the absolute time at which the lease loses authority
     */
    record Lease(String taskId, String ownerId, long fencingToken, long expiryMillis) {

        public Lease {
            Objects.requireNonNull(taskId);
            Objects.requireNonNull(ownerId);
            if (fencingToken < 0L) {
                throw new IllegalArgumentException("fencing token must be non-negative");
            }
            if (expiryMillis < 0L) {
                throw new IllegalArgumentException("expiry time must be non-negative");
            }
        }
    }

    /**
     * A task and the lease under which it may be processed.
     *
     * @param id the durable task identifier
     * @param state the task-specific persistent state
     * @param lease the lease acquired for this processing attempt
     */
    record LeasedTask<S>(String id, S state, Lease lease) {

        public LeasedTask {
            Objects.requireNonNull(id);
            Objects.requireNonNull(state);
            Objects.requireNonNull(lease);
            if (id.equals(lease.taskId()) == false) {
                throw new IllegalArgumentException("task ID [" + id + "] does not match lease task ID [" + lease.taskId() + "]");
            }
        }
    }

    /** Indicates that an operation was rejected because its lease no longer owns the task. */
    class LeaseLostException extends RuntimeException {

        /** Creates an exception describing why the lease no longer grants authority. */
        public LeaseLostException(String message) {
            super(message);
        }

        /** Creates an exception describing why the lease no longer grants authority. */
        public LeaseLostException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** Claims up to {@code maxTasks} tasks for {@code ownerId}. */
    void claim(String ownerId, int maxTasks, TimeValue leaseDuration, ActionListener<List<LeasedTask<S>>> listener);

    /** Renews a live lease and returns its new expiry. */
    void renew(Lease lease, TimeValue leaseDuration, ActionListener<Lease> listener);

    /** Replaces the task-specific persistent state while retaining the lease. */
    void modify(Lease lease, S newState, ActionListener<S> listener);

    /** Replaces the task-specific persistent state and makes the task terminal. */
    void finish(Lease lease, S finalState, ActionListener<S> listener);

    /**
     * Voluntarily gives up the lease and makes the task available immediately. This operation must be idempotent for a task that remains
     * available with the same fencing token.
     */
    void release(Lease lease, ActionListener<Void> listener);
}
