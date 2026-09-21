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
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.snapshots.restore.TaskQueue.Lease;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class TaskRuntime<S> {
    public interface Handle<S> {
        S state();

        void update(S newState, boolean terminal, ActionListener<Void> listener);
    }

    public interface Task<S> {
        void process(Handle<S> handle);

        void close(boolean cancelled);
    }

    protected abstract Task<S> taskInstance();

    protected TaskRuntime(
        ClusterService clusterService,
        TaskQueue<S> queue,
        ThreadPool threadPool,
        Executor executor,
        String workerId,
        int maxConcurrentTasks,
        TimeValue leaseDuration,
        TimeValue claimInterval,
        TimeValue shutdownGracePeriod
    ) {
        Objects.requireNonNull(clusterService);
        this.queue = Objects.requireNonNull(queue);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.executor = Objects.requireNonNull(executor);
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

    private static final Logger logger = LogManager.getLogger(TaskRuntime.class);

    private final TaskQueue<S> queue;
    private final ThreadPool threadPool;
    private final Executor executor;
    private final String workerId;
    private final int maxConcurrentTasks;
    private final TimeValue leaseDuration;
    private final TimeValue claimInterval;

    private final Object mutex = new Object();
    private final Set<ActiveTask> activeTasks = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean claimInProgress = new AtomicBoolean();
    private volatile boolean running;
    private volatile Scheduler.Cancellable claimPoller;

    void startProcessing() {
        running = true;
        try {
            claimPoller = threadPool.scheduleWithFixedDelay(this::claimAvailableTasks, claimInterval, threadPool.generic());
            claimAvailableTasks();
        } catch (EsRejectedExecutionException e) {
            logger.debug("stopping task processor runtime because task queue polling was rejected", e);
            stopProcessing();
        }
    }

    // Exposed for tests that exercise the runtime independently of ClusterService.
    void stopProcessing() {
        synchronized (mutex) {
            running = false;
            claimPoller.cancel();
            for (var task : List.copyOf(activeTasks)) {
                task.cancel(true);
            }
        }
    }

    private void claimAvailableTasks() {
        if (running == false || claimInProgress.compareAndExchange(false, true)) {
            return;
        }

        var capacity = maxConcurrentTasks - activeTasks.size();
        if (capacity <= 0) {
            claimInProgress.set(false);
            return;
        }

        queue.claim(workerId, capacity, leaseDuration, ActionListener.assertOnce(new ActionListener<>() {
            @Override
            public void onResponse(List<Tuple<S, Lease>> claimedTasks) {
                synchronized (mutex) {
                    final long nowMillis = threadPool.absoluteTimeInMillis();
                    for (Tuple<S, Lease> claimedTask : claimedTasks) {
                        final Lease lease = claimedTask.v2();
                        if (running == false || lease.expiryMillis() <= nowMillis) {
                            release(lease);
                            continue;
                        }
                        new ActiveTask().start(lease, claimedTask.v1());
                    }
                    claimInProgress.set(false);
                }
            }

            @Override
            public void onFailure(Exception e) {
                claimInProgress.set(false);
                if (running) {
                    logger.debug("failed to claim queued tasks", e);
                }
            }
        }));
    }

    private void release(Lease lease) {
        queue.release(
            lease,
            ActionListener.wrap(
                ignored -> {},
                failure -> logger.debug(() -> "failed to release lease for task [" + lease.taskId() + "]", failure)
            )
        );
    }

    private class ActiveTask {
        enum Status {
            NEW,
            STARTED,
            DONE,
        }

        private final Object mutex = new Object();
        private Task<S> task;
        private Lease lease;
        private Handle<S> handle;
        private Status status = Status.NEW;
        private Scheduler.Cancellable renewalTimer;
        private Scheduler.Cancellable expiryTimer;

        private void start(Lease lease, S initialState) {
            final List<Runnable> finalizers = new ArrayList<>();
            final boolean releaseLease;
            Exception failure = null;
            synchronized (mutex) {
                if (status == Status.DONE) {
                    releaseLease = true;
                } else if (status != Status.NEW) {
                    return;
                } else {
                    releaseLease = false;
                    try {
                        this.lease = lease;
                        finalizers.add(() -> cancel(true));
                        this.task = taskInstance();

                        final long remainingMillis = lease.expiryMillis() - threadPool.absoluteTimeInMillis();
                        if (remainingMillis <= 0L) {
                            throw new IllegalStateException();
                        }

                        this.renewalTimer = threadPool.schedule(
                            this::onRenewalTimer,
                            TimeValue.timeValueMillis(Math.max(1L, remainingMillis / 2L)),
                            threadPool.generic()
                        );

                        this.expiryTimer = threadPool.schedule(
                            this::onExpiryTimer,
                            TimeValue.timeValueMillis(remainingMillis),
                            threadPool.generic()
                        );

                        this.handle = stubHandle(initialState);
                        executor.execute(() -> task.process(handle));

                        status = Status.STARTED;
                        activeTasks.add(this);
                        finalizers.clear();
                    } catch (Exception e) {
                        failure = e;
                    }
                }
            }
            if (failure != null) {
                for (int i = finalizers.size() - 1; i >= 0; i--) {
                    try {
                        finalizers.get(i).run();
                    } catch (Exception suppressed) {
                        failure.addSuppressed(suppressed);
                    }
                }
                logger.debug(() -> "failed to start task [" + lease.taskId() + "]", failure);
            }
            if (releaseLease) {
                release(lease);
            }
        }

        private void cancel(boolean releaseLease) {
            final boolean closeTask;
            synchronized (mutex) {
                if (status == Status.DONE) {
                    return;
                }
                closeTask = status == Status.STARTED;
                status = Status.DONE;
            }
            if (renewalTimer != null) {
                renewalTimer.cancel();
            }
            if (expiryTimer != null) {
                expiryTimer.cancel();
            }
            if (closeTask) {
                try {
                    task.close(true);
                } catch (Exception e) {
                    logger.warn(() -> "failed to close task [" + lease.taskId() + "]", e);
                }
            }
            activeTasks.remove(this);
            if (releaseLease) {
                release(lease);
            }
        }

        private void onRenewalTimer() {
            // TODO
        }

        private void onExpiryTimer() {
            cancel(false);
        }

        private Handle<S> stubHandle(S initialState) {
            // TODO: real handle
            return new Handle<>() {
                @Override
                public S state() {
                    return initialState;
                }

                @Override
                public void update(S newState, boolean terminal, ActionListener<Void> listener) {
                    throw new UnsupportedOperationException("handle update not implemented");
                }
            };
        }
    }
}
