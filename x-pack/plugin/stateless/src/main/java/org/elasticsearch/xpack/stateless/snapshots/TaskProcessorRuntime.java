/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.snapshots.TaskQueue.Lease;
import org.elasticsearch.xpack.stateless.snapshots.TaskQueue.LeasedTask;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * Runs a {@link TaskProcessor} against tasks claimed from a {@link TaskQueue}, with bounded concurrency and automatic lease management.
 */
public final class TaskProcessorRuntime<S> extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(TaskProcessorRuntime.class);

    private final TaskQueue<S> queue;
    private final TaskProcessor<S> processor;
    private final ThreadPool threadPool;
    private final Executor processorExecutor;
    private final String workerId;
    private final int maxConcurrentTasks;
    private final TimeValue leaseDuration;
    private final TimeValue claimInterval;
    private final Map<String, TaskExecutionImpl<S>> active = new HashMap<>();

    // The following fields are guarded by this.
    private boolean running;
    private boolean claimInProgress;
    private Scheduler.Cancellable nextClaim;

    /**
     * Creates a runtime for one task type and processor.
     */
    public TaskProcessorRuntime(
        TaskQueue<S> queue,
        TaskProcessor<S> processor,
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
        synchronized (this) {
            running = true;
        }
        fillCapacity();
    }

    @Override
    protected void doStop() {
        final List<TaskExecutionImpl<S>> executions;
        synchronized (this) {
            running = false;
            if (nextClaim != null) {
                nextClaim.cancel();
                nextClaim = null;
            }
            executions = List.copyOf(active.values());
        }
        executions.forEach(TaskExecutionImpl::runtimeStopped);
    }

    @Override
    protected void doClose() {}

    private void fillCapacity() {
        final int capacity;
        synchronized (this) {
            if (running == false || claimInProgress) {
                return;
            }
            capacity = maxConcurrentTasks - active.size();
            if (capacity <= 0) {
                return;
            }
            claimInProgress = true;
        }

        final ActionListener<List<LeasedTask<S>>> listener = ActionListener.assertOnce(
            ActionListener.wrap(this::claimsCompleted, this::claimFailed)
        );
        try {
            queue.claim(workerId, capacity, leaseDuration, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void claimsCompleted(List<LeasedTask<S>> claimedTasks) {
        Objects.requireNonNull(claimedTasks);
        final List<TaskExecutionImpl<S>> toStart = new ArrayList<>();
        final List<Lease> toRelease = new ArrayList<>();
        final boolean shouldPollLater;

        synchronized (this) {
            claimInProgress = false;
            if (running == false) {
                claimedTasks.forEach(task -> toRelease.add(task.lease()));
            } else {
                int remainingCapacity = maxConcurrentTasks - active.size();
                for (LeasedTask<S> task : claimedTasks) {
                    if (remainingCapacity > 0 && active.containsKey(task.id()) == false) {
                        var execution = new TaskExecutionImpl<>(task, queue, threadPool, leaseDuration, this::executionEnded);
                        active.put(task.id(), execution);
                        toStart.add(execution);
                        remainingCapacity--;
                    } else {
                        toRelease.add(task.lease());
                    }
                }
            }
            shouldPollLater = running && toStart.isEmpty();
        }

        toRelease.forEach(this::releaseUnstartedLease);
        toStart.forEach(this::startExecution);

        if (shouldPollLater) {
            scheduleNextClaim();
        } else {
            fillCapacity();
        }
    }

    private void claimFailed(Exception failure) {
        synchronized (this) {
            claimInProgress = false;
            if (running == false) {
                return;
            }
        }
        logger.debug("failed to claim queued tasks", failure);
        scheduleNextClaim();
    }

    private void scheduleNextClaim() {
        synchronized (this) {
            if (running == false || nextClaim != null || claimInProgress || active.size() >= maxConcurrentTasks) {
                return;
            }
            try {
                nextClaim = threadPool.schedule(() -> {
                    synchronized (TaskProcessorRuntime.this) {
                        nextClaim = null;
                    }
                    fillCapacity();
                }, claimInterval, threadPool.generic());
            } catch (Exception e) {
                logger.debug("failed to schedule the next task claim", e);
            }
        }
    }

    private void startExecution(TaskExecutionImpl<S> execution) {
        if (execution.start() == false) {
            return;
        }
        try {
            processorExecutor.execute(() -> {
                if (execution.canProcess() == false) {
                    return;
                }
                try {
                    processor.process(execution);
                } catch (Exception e) {
                    execution.processorFailed(e);
                }
            });
        } catch (Exception e) {
            execution.processorFailed(e);
        }
    }

    private void executionEnded(TaskExecutionImpl<S> execution) {
        final boolean refill;
        synchronized (this) {
            active.remove(execution.taskId(), execution);
            refill = running;
        }
        if (refill) {
            fillCapacity();
        }
    }

    private void releaseUnstartedLease(Lease lease) {
        final ActionListener<Void> listener = ActionListener.wrap(
            ignored -> {},
            e -> logger.debug(() -> "failed to release unstarted task lease [" + lease.taskId() + "]", e)
        );
        try {
            queue.release(lease, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    // Exposed for tests.
    synchronized int activeTaskCount() {
        return active.size();
    }
}
