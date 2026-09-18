/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.action.ActionListener;

/** Performs one kind of queued task and stops work whose lease is no longer valid. */
public interface Task<S> {

    /** Performs the task's work. This method may block for the lifetime of the task. */
    void process(TaskHandle<S> task) throws Exception;

    /** Stops work for the task. This method may run concurrently with {@link #process}. */
    void cancel(TaskHandle<S> task);

    /**
     * Access to one leased task. A task may have internal concurrency, but must submit persistent state changes sequentially: only one
     * call to {@link #modify} or {@link #finish} may be outstanding.
     */
    interface TaskHandle<S> {

        /** Returns the state from the latest successful persistent state change. */
        S state();

        /** Persists a non-terminal state change. */
        void modify(S newState, ActionListener<S> listener);

        /** Persists the final state and completes this task. */
        void finish(S finalState, ActionListener<S> listener);
    }
}
