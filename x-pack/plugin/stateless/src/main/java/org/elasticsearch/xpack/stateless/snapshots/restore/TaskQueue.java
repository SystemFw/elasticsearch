/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.restore;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.Result;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.Map;

/**
 * Durable queue operations needed by {@link TaskProcessorRuntime}. A nonterminal task is available to claim when it has no lease or its
 * lease has expired. A task with a live lease and a terminal task are not available to claim.
 * <p>
 * Implementations must validate the lease identity for every operation other than {@link #claim}. Renewal and state changes must also
 * require the task to be nonterminal. Finishing a task atomically stores its final state, makes it terminal, and removes its lease. The
 * final state determines whether the terminal outcome represents success or failure. All failures must be reported to the supplied
 * listener; these methods must not throw exceptions.
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
    record Lease(String taskId, String ownerId, long fencingToken, long expiryMillis) {}

    /**
     * Claims up to {@code maxTasks} distinct available tasks for {@code ownerId}, replacing any expired leases. Each task must occur at
     * most once in the response.
     */
    void claim(String ownerId, int maxTasks, TimeValue leaseDuration, ActionListener<List<Tuple<S, Lease>>> listener);

    /**
     * Renews a live lease and returns the renewed lease. The queue owns any retries for transient failures; callers treat a reported
     * failure as final.
     */
    void renew(Lease lease, TimeValue leaseDuration, ActionListener<Lease> listener);

    /**
     * Renews live leases in bulk. The response must contain one result keyed by each input lease. A request-level failure is reported to
     * {@code listener}; failures affecting individual leases are returned as item results. The queue owns any retries for transient
     * failures and callers treat reported failures as final. Internal retries must not hold successful item results until their previously
     * known leases expire: unresolved items must be returned as failures early enough for the caller to process successful renewals before
     * the earliest input lease expires.
     */
    void renew(List<Lease> leases, TimeValue leaseDuration, ActionListener<Map<Lease, Result<Lease, Exception>>> listener);

    /**
     * Replaces the task-specific persistent state, optionally making the task terminal and removing its lease. The queue owns any retries
     * for transient failures; callers treat a reported failure as final and stop processing the task.
     */
    void update(Lease lease, S newState, boolean terminal, ActionListener<S> listener);

    /**
     * Voluntarily gives up the lease and makes the task available immediately. This operation must be idempotent for a task that remains
     * available with the same fencing token.
     */
    void release(Lease lease, ActionListener<Void> listener);
}
