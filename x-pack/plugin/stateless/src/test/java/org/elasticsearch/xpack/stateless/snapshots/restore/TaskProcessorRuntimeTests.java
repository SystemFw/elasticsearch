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
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.snapshots.restore.Task.TaskHandle;
import org.elasticsearch.xpack.stateless.snapshots.restore.TaskQueue.Lease;
import org.mockito.ArgumentCaptor;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TaskProcessorRuntimeTests extends ESTestCase {

    public void testProcessorModifiesStateSequentiallyAndFinishes() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var queue = new TestTaskQueue(deterministicTaskQueue);
        queue.add("task", "initial");
        queue.deferModifications = true;
        var executionRef = new AtomicReference<TaskHandle<String>>();
        var runtime = newRuntime(deterministicTaskQueue, queue, executionRef::set);

        runtime.startProcessing();
        deterministicTaskQueue.runAllRunnableTasks();

        var execution = executionRef.get();
        assertNotNull(execution);
        assertThat(execution.state(), equalTo("initial"));
        assertThat(runtime.activeTaskCount(), equalTo(1));

        var firstResult = new AtomicReference<String>();
        execution.update("modified", false, ActionListener.wrap(firstResult::set, e -> fail(e.getMessage())));

        var concurrentFailure = new AtomicReference<Exception>();
        execution.update(
            "not-written",
            false,
            ActionListener.wrap(ignored -> fail("second modification unexpectedly succeeded"), concurrentFailure::set)
        );
        assertThat(concurrentFailure.get(), instanceOf(IllegalStateException.class));
        assertThat(execution.state(), equalTo("initial"));

        queue.completeModification();
        assertThat(firstResult.get(), equalTo("modified"));
        assertThat(execution.state(), equalTo("modified"));

        var finalResult = new AtomicReference<String>();
        execution.update("finished", true, ActionListener.wrap(finalResult::set, e -> fail(e.getMessage())));
        assertThat(finalResult.get(), equalTo("finished"));
        assertThat(runtime.activeTaskCount(), equalTo(0));

        runtime.stopProcessing();
    }

    public void testUpdateFailureCancelsTask() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var queue = new TestTaskQueue(deterministicTaskQueue);
        queue.add("task", "initial");
        queue.failUpdates = true;
        var executionRef = new AtomicReference<TaskHandle<String>>();
        var cancelledTask = new AtomicReference<TaskHandle<String>>();
        var runtime = newRuntime(deterministicTaskQueue, queue, processor(executionRef::set, cancelledTask::set));

        runtime.startProcessing();
        deterministicTaskQueue.runAllRunnableTasks();

        var updateFailure = new AtomicReference<Exception>();
        executionRef.get()
            .update("modified", false, ActionListener.wrap(ignored -> fail("update unexpectedly succeeded"), updateFailure::set));

        assertSame(queue.updateFailure, updateFailure.get());
        assertSame(executionRef.get(), cancelledTask.get());
        assertThat(runtime.activeTaskCount(), equalTo(0));

        runtime.stopProcessing();
    }

    public void testLeaseIsRenewedWhileProcessorIsActive() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var queue = new TestTaskQueue(deterministicTaskQueue);
        queue.add("task", "initial");
        var executionRef = new AtomicReference<TaskHandle<String>>();
        var runtime = newRuntime(deterministicTaskQueue, queue, executionRef::set);

        runtime.startProcessing();
        deterministicTaskQueue.runAllRunnableTasks();
        assertNotNull(executionRef.get());

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();

        assertThat(queue.renewedLeases.size(), equalTo(1));
        assertThat(queue.singleRenewCount, equalTo(0));
        assertThat(queue.bulkRenewCount, equalTo(1));
        assertThat(runtime.activeTaskCount(), equalTo(1));

        runtime.stopProcessing();
        assertThat(queue.releasedLeases.size(), equalTo(1));
    }

    public void testNearDueLeasesAreRenewedInOneBatch() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var queue = new TestTaskQueue(deterministicTaskQueue);
        queue.add("task-1", "initial");
        queue.add("task-2", "initial");
        queue.claimedLeaseExpiryStepMillis = 10L;
        var runtime = newRuntime(deterministicTaskQueue, queue, processor(ignored -> {}, ignored -> {}), 2);

        runtime.startProcessing();
        deterministicTaskQueue.runAllRunnableTasks();

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();

        assertThat(queue.renewedLeases.size(), equalTo(2));
        assertNotEquals(queue.renewedLeases.get(0).expiryMillis(), queue.renewedLeases.get(1).expiryMillis());
        assertThat(queue.singleRenewCount, equalTo(0));
        assertThat(queue.bulkRenewCount, equalTo(1));

        runtime.stopProcessing();
    }

    public void testBulkRenewalResultsAreAppliedIndependently() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var queue = new TestTaskQueue(deterministicTaskQueue);
        queue.add("task-1", "lost");
        queue.add("task-2", "renewed");
        queue.renewalFailureTaskId = "task-1";
        var cancelledStates = new ArrayList<String>();
        var runtime = newRuntime(deterministicTaskQueue, queue, processor(ignored -> {}, task -> cancelledStates.add(task.state())), 2);

        runtime.startProcessing();
        deterministicTaskQueue.runAllRunnableTasks();

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();

        assertThat(queue.singleRenewCount, equalTo(0));
        assertThat(queue.bulkRenewCount, equalTo(1));
        assertThat(cancelledStates, equalTo(List.of("lost")));
        assertThat(runtime.activeTaskCount(), equalTo(1));

        runtime.stopProcessing();
    }

    public void testPartialClaimWaitsBeforePollingAgain() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var queue = new TestTaskQueue(deterministicTaskQueue);
        queue.add("task", "initial");
        var runtime = newRuntime(deterministicTaskQueue, queue, processor(ignored -> {}, ignored -> {}), 2);

        runtime.startProcessing();

        assertThat(queue.claimCount, equalTo(1));

        runtime.stopProcessing();
    }

    public void testHangingRenewalLosesLeaseAtLastConfirmedExpiry() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var queue = new TestTaskQueue(deterministicTaskQueue);
        queue.add("task", "initial");
        queue.completeRenewals = false;
        var executionRef = new AtomicReference<TaskHandle<String>>();
        var cancelledTask = new AtomicReference<TaskHandle<String>>();
        var runtime = newRuntime(deterministicTaskQueue, queue, processor(executionRef::set, cancelledTask::set));

        runtime.startProcessing();
        deterministicTaskQueue.runAllRunnableTasks();

        deterministicTaskQueue.advanceTime(); // Renewal starts halfway through the lease and does not complete.
        deterministicTaskQueue.runAllRunnableTasks();
        assertNull(cancelledTask.get());

        deterministicTaskQueue.advanceTime(); // The expiry watchdog fires at the last confirmed expiry.
        deterministicTaskQueue.runAllRunnableTasks();
        assertSame(executionRef.get(), cancelledTask.get());
        assertThat(runtime.activeTaskCount(), equalTo(0));

        runtime.stopProcessing();
    }

    public void testRenewalFailureCancelsTask() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var queue = new TestTaskQueue(deterministicTaskQueue);
        queue.add("task", "initial");
        queue.failRenewals = true;
        var executionRef = new AtomicReference<TaskHandle<String>>();
        var cancelledTask = new AtomicReference<TaskHandle<String>>();
        var runtime = newRuntime(deterministicTaskQueue, queue, processor(executionRef::set, cancelledTask::set));

        runtime.startProcessing();
        deterministicTaskQueue.runAllRunnableTasks();
        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();

        assertSame(executionRef.get(), cancelledTask.get());
        assertThat(runtime.activeTaskCount(), equalTo(0));

        runtime.stopProcessing();
    }

    public void testRenewalFailureFailsPendingStateChange() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var queue = new TestTaskQueue(deterministicTaskQueue);
        queue.add("task", "initial");
        queue.deferModifications = true;
        queue.failRenewals = true;
        var executionRef = new AtomicReference<TaskHandle<String>>();
        var cancelledTask = new AtomicReference<TaskHandle<String>>();
        var runtime = newRuntime(deterministicTaskQueue, queue, processor(executionRef::set, cancelledTask::set));

        runtime.startProcessing();
        deterministicTaskQueue.runAllRunnableTasks();
        var execution = executionRef.get();
        var stateChangeFailure = new AtomicReference<Exception>();
        execution.update(
            "modified",
            false,
            ActionListener.wrap(ignored -> fail("state change unexpectedly succeeded"), stateChangeFailure::set)
        );

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();

        assertSame(queue.renewalFailure, stateChangeFailure.get());
        assertSame(execution, cancelledTask.get());
        assertThat(runtime.activeTaskCount(), equalTo(0));

        runtime.stopProcessing();
    }

    public void testRenewalFailureFailsPendingFinishImmediately() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var queue = new TestTaskQueue(deterministicTaskQueue);
        queue.add("task", "initial");
        queue.deferFinishes = true;
        queue.failRenewals = true;
        var executionRef = new AtomicReference<TaskHandle<String>>();
        var cancelledTask = new AtomicReference<TaskHandle<String>>();
        var runtime = newRuntime(deterministicTaskQueue, queue, processor(executionRef::set, cancelledTask::set));

        runtime.startProcessing();
        deterministicTaskQueue.runAllRunnableTasks();
        var execution = executionRef.get();
        var finishFailure = new AtomicReference<Exception>();
        execution.update("finished", true, ActionListener.wrap(ignored -> fail("finish unexpectedly succeeded"), finishFailure::set));

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();
        assertSame(queue.renewalFailure, finishFailure.get());
        assertSame(execution, cancelledTask.get());
        assertThat(runtime.activeTaskCount(), equalTo(0));

        // The queue operation may still have committed even though its successful response arrived after cancellation.
        queue.completeFinish();
        assertThat(queue.states.get("task"), equalTo("finished"));

        runtime.stopProcessing();
    }

    public void testStopReleasesActiveLease() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var queue = new TestTaskQueue(deterministicTaskQueue);
        queue.add("task", "initial");
        var executionRef = new AtomicReference<TaskHandle<String>>();
        var cancelledTask = new AtomicReference<TaskHandle<String>>();
        var runtime = newRuntime(deterministicTaskQueue, queue, processor(executionRef::set, cancelledTask::set));

        runtime.startProcessing();
        deterministicTaskQueue.runAllRunnableTasks();
        runtime.stopProcessing();

        assertSame(executionRef.get(), cancelledTask.get());
        assertThat(queue.releasedLeases.size(), equalTo(1));
        assertThat(runtime.activeTaskCount(), equalTo(0));
    }

    public void testClaimCompletingAfterStopIsReleasedWithoutStartingProcessor() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var queue = new TestTaskQueue(deterministicTaskQueue);
        queue.add("task", "initial");
        queue.deferClaims = true;
        var processCalled = new AtomicBoolean();
        var runtime = newRuntime(deterministicTaskQueue, queue, execution -> processCalled.set(true));

        runtime.startProcessing();
        runtime.stopProcessing();
        queue.completeClaim();
        deterministicTaskQueue.runAllRunnableTasks();

        assertFalse(processCalled.get());
        assertThat(queue.releasedLeases.size(), equalTo(1));
    }

    public void testSchedulingRejectionStopsProcessing() {
        var deterministicTaskQueue = new RejectingDeterministicTaskQueue();
        deterministicTaskQueue.rejectScheduling = true;
        var queue = new TestTaskQueue(deterministicTaskQueue);
        queue.add("task", "initial");
        var processCalled = new AtomicBoolean();
        var cancelledTask = new AtomicReference<TaskHandle<String>>();
        var runtime = newRuntime(deterministicTaskQueue, queue, processor(ignored -> processCalled.set(true), cancelledTask::set));

        runtime.startProcessing();

        assertFalse(processCalled.get());
        assertNotNull(cancelledTask.get());
        assertThat(cancelledTask.get().state(), equalTo("initial"));
        assertThat(queue.releasedLeases.size(), equalTo(1));
        assertThat(runtime.activeTaskCount(), equalTo(0));

        runtime.stopProcessing();
        assertThat(queue.releasedLeases.size(), equalTo(1));
    }

    public void testFollowsClusterServiceLifecycle() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var queue = new TestTaskQueue(deterministicTaskQueue);
        queue.add("task", "initial");
        var executionRef = new AtomicReference<TaskHandle<String>>();
        var cancelledTask = new AtomicReference<TaskHandle<String>>();
        // A real ClusterService starts during test construction, before the runtime can register its lifecycle listener.
        var clusterService = mock(ClusterService.class);
        var runtime = newRuntime(deterministicTaskQueue, queue, processor(executionRef::set, cancelledTask::set), 1, clusterService);
        var lifecycleListener = ArgumentCaptor.forClass(LifecycleListener.class);
        verify(clusterService).addLifecycleListener(lifecycleListener.capture());

        assertThat(queue.claimCount, equalTo(0));
        lifecycleListener.getValue().afterStart();
        deterministicTaskQueue.runAllRunnableTasks();
        assertNotNull(executionRef.get());

        lifecycleListener.getValue().beforeStop();
        assertSame(executionRef.get(), cancelledTask.get());
        assertThat(queue.releasedLeases.size(), equalTo(1));
        assertThat(runtime.activeTaskCount(), equalTo(0));
    }

    private static TaskProcessorRuntime<String> newRuntime(
        DeterministicTaskQueue deterministicTaskQueue,
        TestTaskQueue queue,
        Consumer<TaskHandle<String>> processor
    ) {
        return newRuntime(deterministicTaskQueue, queue, processor(processor, ignored -> {}));
    }

    private static TaskProcessorRuntime<String> newRuntime(
        DeterministicTaskQueue deterministicTaskQueue,
        TestTaskQueue queue,
        Task<String> processor
    ) {
        return newRuntime(deterministicTaskQueue, queue, processor, 1);
    }

    private static TaskProcessorRuntime<String> newRuntime(
        DeterministicTaskQueue deterministicTaskQueue,
        TestTaskQueue queue,
        Task<String> processor,
        int maxConcurrentTasks
    ) {
        return newRuntime(deterministicTaskQueue, queue, processor, maxConcurrentTasks, mock(ClusterService.class));
    }

    private static TaskProcessorRuntime<String> newRuntime(
        DeterministicTaskQueue deterministicTaskQueue,
        TestTaskQueue queue,
        Task<String> processor,
        int maxConcurrentTasks,
        ClusterService clusterService
    ) {
        var threadPool = deterministicTaskQueue.getThreadPool();
        return new TaskProcessorRuntime<>(
            clusterService,
            queue,
            processor,
            threadPool,
            threadPool.generic(),
            "worker",
            maxConcurrentTasks,
            TimeValue.timeValueMillis(100),
            TimeValue.timeValueSeconds(1)
        );
    }

    private static Task<String> processor(Consumer<TaskHandle<String>> process, Consumer<TaskHandle<String>> cancel) {
        return new Task<>() {
            @Override
            public void process(TaskHandle<String> task) {
                process.accept(task);
            }

            @Override
            public void cancel(TaskHandle<String> task) {
                cancel.accept(task);
            }
        };
    }

    private static class RejectingDeterministicTaskQueue extends DeterministicTaskQueue {

        private boolean rejectScheduling;

        @Override
        public ThreadPool getThreadPool() {
            return new DeterministicThreadPool(Function.identity()) {
                @Override
                public Scheduler.ScheduledCancellable schedule(Runnable command, TimeValue delay, Executor executor) {
                    if (rejectScheduling) {
                        throw new EsRejectedExecutionException("simulated scheduler shutdown", true);
                    }
                    return super.schedule(command, delay, executor);
                }
            };
        }
    }

    private static class TestTaskQueue implements TaskQueue<String> {

        private final DeterministicTaskQueue deterministicTaskQueue;
        private final ArrayDeque<Map.Entry<String, String>> readyTasks = new ArrayDeque<>();
        private final Map<String, String> states = new HashMap<>();
        private final List<Lease> renewedLeases = new ArrayList<>();
        private final List<Lease> releasedLeases = new ArrayList<>();
        private int claimCount;
        private int singleRenewCount;
        private int bulkRenewCount;
        private long nextFencingToken;
        private boolean deferClaims;
        private boolean deferModifications;
        private boolean deferFinishes;
        private boolean completeRenewals = true;
        private boolean failRenewals;
        private boolean failUpdates;
        private String renewalFailureTaskId;
        private final RuntimeException renewalFailure = new RuntimeException("simulated renewal failure");
        private final RuntimeException updateFailure = new RuntimeException("simulated update failure");
        private long claimedLeaseExpiryStepMillis;
        private ActionListener<List<Tuple<String, Lease>>> pendingClaim;
        private PendingModification pendingModification;
        private PendingModification pendingFinish;

        private TestTaskQueue(DeterministicTaskQueue deterministicTaskQueue) {
            this.deterministicTaskQueue = deterministicTaskQueue;
        }

        void add(String id, String state) {
            states.put(id, state);
            readyTasks.add(Map.entry(id, state));
        }

        @Override
        public void claim(String ownerId, int maxTasks, TimeValue leaseDuration, ActionListener<List<Tuple<String, Lease>>> listener) {
            claimCount++;
            if (deferClaims) {
                assertNull(pendingClaim);
                pendingClaim = listener;
                return;
            }
            listener.onResponse(claimTasks(ownerId, maxTasks, leaseDuration));
        }

        void completeClaim() {
            var listener = pendingClaim;
            pendingClaim = null;
            listener.onResponse(claimTasks("worker", 1, TimeValue.timeValueMillis(100)));
        }

        private List<Tuple<String, Lease>> claimTasks(String ownerId, int maxTasks, TimeValue leaseDuration) {
            var result = new ArrayList<Tuple<String, Lease>>();
            while (result.size() < maxTasks && readyTasks.isEmpty() == false) {
                var task = readyTasks.remove();
                var lease = new Lease(
                    task.getKey(),
                    ownerId,
                    nextFencingToken++,
                    deterministicTaskQueue.getCurrentTimeMillis() + leaseDuration.millis() + result.size() * claimedLeaseExpiryStepMillis
                );
                result.add(new Tuple<>(states.get(task.getKey()), lease));
            }
            return result;
        }

        @Override
        public void renew(Lease lease, TimeValue leaseDuration, ActionListener<Lease> listener) {
            singleRenewCount++;
            renewedLeases.add(lease);
            if (failRenewals) {
                listener.onFailure(renewalFailure);
            } else if (completeRenewals) {
                listener.onResponse(
                    new Lease(
                        lease.taskId(),
                        lease.ownerId(),
                        lease.fencingToken(),
                        deterministicTaskQueue.getCurrentTimeMillis() + leaseDuration.millis()
                    )
                );
            }
        }

        @Override
        public void renew(List<Lease> leases, TimeValue leaseDuration, ActionListener<List<Result<Lease, Exception>>> listener) {
            bulkRenewCount++;
            renewedLeases.addAll(leases);
            if (completeRenewals) {
                listener.onResponse(
                    leases.stream()
                        .map(
                            lease -> failRenewals || lease.taskId().equals(renewalFailureTaskId)
                                ? Result.<Lease, Exception>failure(renewalFailure)
                                : Result.<Lease, Exception>of(
                                    new Lease(
                                        lease.taskId(),
                                        lease.ownerId(),
                                        lease.fencingToken(),
                                        deterministicTaskQueue.getCurrentTimeMillis() + leaseDuration.millis()
                                    )
                                )
                        )
                        .toList()
                );
            }
        }

        @Override
        public void update(Lease lease, String newState, boolean terminal, ActionListener<String> listener) {
            if (failUpdates) {
                listener.onFailure(updateFailure);
            } else if (terminal) {
                if (deferFinishes) {
                    assertNull(pendingFinish);
                    pendingFinish = new PendingModification(lease.taskId(), newState, listener);
                } else {
                    states.put(lease.taskId(), newState);
                    listener.onResponse(newState);
                }
            } else if (deferModifications) {
                assertNull(pendingModification);
                pendingModification = new PendingModification(lease.taskId(), newState, listener);
            } else {
                states.put(lease.taskId(), newState);
                listener.onResponse(newState);
            }
        }

        void completeModification() {
            var modification = pendingModification;
            pendingModification = null;
            states.put(modification.taskId, modification.state);
            modification.listener.onResponse(modification.state);
        }

        void completeFinish() {
            var finish = pendingFinish;
            pendingFinish = null;
            states.put(finish.taskId, finish.state);
            finish.listener.onResponse(finish.state);
        }

        @Override
        public void release(Lease lease, ActionListener<Void> listener) {
            releasedLeases.add(lease);
            listener.onResponse(null);
        }

        private record PendingModification(String taskId, String state, ActionListener<String> listener) {}
    }
}
