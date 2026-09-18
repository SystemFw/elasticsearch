/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.action.ActionListener;

/**
 * Performs the work for one leased task. Returning from {@link #process} does not finish the task; the processor must eventually call
 * {@link TaskExecution#finish} unless it loses its lease.
 */
public interface TaskProcessor<S> {

    /** Starts processing one lease incarnation. */
    void process(TaskExecution<S> execution);

    /**
     * Processor-facing access to one lease incarnation. A processor may have internal concurrency, but must submit persistent state
     * changes sequentially: only one call to {@link #modify} or {@link #finish} may be outstanding.
     */
    interface TaskExecution<S> {

        /** Returns the state from the latest successful persistent state change. */
        S state();

        /** Persists a non-terminal state change. */
        void modify(S newState, ActionListener<S> listener);

        /** Persists the final state and completes this execution. */
        void finish(S finalState, ActionListener<S> listener);

        /** Registers a callback that runs if this execution loses authority before finishing. */
        void addLeaseLostListener(Runnable listener);
    }
}
