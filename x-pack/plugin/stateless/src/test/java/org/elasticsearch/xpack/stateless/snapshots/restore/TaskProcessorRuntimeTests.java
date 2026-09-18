/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.restore;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.snapshots.restore.Task.TaskHandle;
import org.elasticsearch.xpack.stateless.snapshots.restore.TaskQueue.Lease;
import org.elasticsearch.xpack.stateless.snapshots.restore.TaskQueue.LeaseLostException;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class TaskProcessorRuntimeTests extends ESTestCase {

    public void testProcessorModifiesStateSequentiallyAndFinishes() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var queue = new TestTaskQueue(deterministicTaskQueue);
        queue.add("task", "initial");
        queue.deferModifications = true;
        var executionRef = new AtomicReference<TaskHandle<String>>();
        var runtime = newRuntime(deterministicTaskQueue, queue, executionRef::set);

        runtime.start();
        deterministicTaskQueue.runAllRunnableTasks();

        var execution = executionRef.get();
        assertNotNull(execution);
        assertThat(execution.state(), equalTo("initial"));
        assertThat(runtime.activeTaskCount(), equalTo(1));

        var firstResult = new AtomicReference<String>();
        execution.modify("modified", ActionListener.wrap(firstResult::set, e -> fail(e.getMessage())));

        var concurrentFailure = new AtomicReference<Exception>();
        execution.modify(
            "not-written",
            ActionListener.wrap(ignored -> fail("second modification unexpectedly succeeded"), concurrentFailure::set)
        );
        assertThat(concurrentFailure.get(), instanceOf(IllegalStateException.class));
        assertThat(execution.state(), equalTo("initial"));

        queue.completeModification();
        assertThat(firstResult.get(), equalTo("modified"));
        assertThat(execution.state(), equalTo("modified"));

        var finalResult = new AtomicReference<String>();
        execution.finish("finished", ActionListener.wrap(finalResult::set, e -> fail(e.getMessage())));
        assertThat(finalResult.get(), equalTo("finished"));
        assertThat(runtime.activeTaskCount(), equalTo(0));

        runtime.stop();
        runtime.close();
    }

    public void testLeaseIsRenewedWhileProcessorIsActive() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var queue = new TestTaskQueue(deterministicTaskQueue);
        queue.add("task", "initial");
        var executionRef = new AtomicReference<TaskHandle<String>>();
        var runtime = newRuntime(deterministicTaskQueue, queue, executionRef::set);

        runtime.start();
        deterministicTaskQueue.runAllRunnableTasks();
        assertNotNull(executionRef.get());

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();

        assertThat(queue.renewedLeases.size(), equalTo(1));
        assertThat(runtime.activeTaskCount(), equalTo(1));

        runtime.stop();
        assertThat(queue.releasedLeases.size(), equalTo(1));
        runtime.close();
    }

    public void testHangingRenewalLosesLeaseAtLastConfirmedExpiry() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var queue = new TestTaskQueue(deterministicTaskQueue);
        queue.add("task", "initial");
        queue.completeRenewals = false;
        var executionRef = new AtomicReference<TaskHandle<String>>();
        var cancelledTask = new AtomicReference<TaskHandle<String>>();
        var runtime = newRuntime(deterministicTaskQueue, queue, processor(executionRef::set, cancelledTask::set));

        runtime.start();
        deterministicTaskQueue.runAllRunnableTasks();

        deterministicTaskQueue.advanceTime(); // Renewal starts halfway through the lease and does not complete.
        deterministicTaskQueue.runAllRunnableTasks();
        assertNull(cancelledTask.get());

        deterministicTaskQueue.advanceTime(); // The expiry watchdog fires at the last confirmed expiry.
        deterministicTaskQueue.runAllRunnableTasks();
        assertSame(executionRef.get(), cancelledTask.get());
        assertThat(runtime.activeTaskCount(), equalTo(0));

        runtime.stop();
        runtime.close();
    }

    public void testFencedRenewalFailsPendingStateChange() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var queue = new TestTaskQueue(deterministicTaskQueue);
        queue.add("task", "initial");
        queue.deferModifications = true;
        queue.failRenewalsWithLeaseLoss = true;
        var executionRef = new AtomicReference<TaskHandle<String>>();
        var cancelledTask = new AtomicReference<TaskHandle<String>>();
        var runtime = newRuntime(deterministicTaskQueue, queue, processor(executionRef::set, cancelledTask::set));

        runtime.start();
        deterministicTaskQueue.runAllRunnableTasks();
        var execution = executionRef.get();
        var stateChangeFailure = new AtomicReference<Exception>();
        execution.modify("modified", ActionListener.wrap(ignored -> fail("state change unexpectedly succeeded"), stateChangeFailure::set));

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();

        assertThat(stateChangeFailure.get(), instanceOf(LeaseLostException.class));
        assertSame(execution, cancelledTask.get());
        assertThat(runtime.activeTaskCount(), equalTo(0));

        runtime.stop();
        runtime.close();
    }

    public void testFencedRenewalWaitsForInFlightFinish() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var queue = new TestTaskQueue(deterministicTaskQueue);
        queue.add("task", "initial");
        queue.deferFinishes = true;
        queue.failRenewalsWithLeaseLoss = true;
        var executionRef = new AtomicReference<TaskHandle<String>>();
        var cancelledTask = new AtomicReference<TaskHandle<String>>();
        var runtime = newRuntime(deterministicTaskQueue, queue, processor(executionRef::set, cancelledTask::set));

        runtime.start();
        deterministicTaskQueue.runAllRunnableTasks();
        var execution = executionRef.get();
        var finishFailure = new AtomicReference<Exception>();
        execution.finish("finished", ActionListener.wrap(ignored -> fail("finish unexpectedly succeeded"), finishFailure::set));

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();
        assertNull(cancelledTask.get());

        queue.failFinish();
        assertThat(finishFailure.get(), instanceOf(LeaseLostException.class));
        assertSame(execution, cancelledTask.get());
        assertThat(runtime.activeTaskCount(), equalTo(0));

        runtime.stop();
        runtime.close();
    }

    public void testStopReleasesActiveLease() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var queue = new TestTaskQueue(deterministicTaskQueue);
        queue.add("task", "initial");
        var executionRef = new AtomicReference<TaskHandle<String>>();
        var cancelledTask = new AtomicReference<TaskHandle<String>>();
        var runtime = newRuntime(deterministicTaskQueue, queue, processor(executionRef::set, cancelledTask::set));

        runtime.start();
        deterministicTaskQueue.runAllRunnableTasks();
        runtime.stop();

        assertSame(executionRef.get(), cancelledTask.get());
        assertThat(queue.releasedLeases.size(), equalTo(1));
        assertThat(runtime.activeTaskCount(), equalTo(0));
        runtime.close();
    }

    public void testClaimCompletingAfterStopIsReleasedWithoutStartingProcessor() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var queue = new TestTaskQueue(deterministicTaskQueue);
        queue.add("task", "initial");
        queue.deferClaims = true;
        var processCalled = new AtomicBoolean();
        var runtime = newRuntime(deterministicTaskQueue, queue, execution -> processCalled.set(true));

        runtime.start();
        runtime.stop();
        queue.completeClaim();
        deterministicTaskQueue.runAllRunnableTasks();

        assertFalse(processCalled.get());
        assertThat(queue.releasedLeases.size(), equalTo(1));
        runtime.close();
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
        var threadPool = deterministicTaskQueue.getThreadPool();
        return new TaskProcessorRuntime<>(
            queue,
            processor,
            threadPool,
            threadPool.generic(),
            "worker",
            1,
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

    private static class TestTaskQueue implements TaskQueue<String> {

        private final DeterministicTaskQueue deterministicTaskQueue;
        private final ArrayDeque<Map.Entry<String, String>> readyTasks = new ArrayDeque<>();
        private final Map<String, String> states = new HashMap<>();
        private final List<Lease> renewedLeases = new ArrayList<>();
        private final List<Lease> releasedLeases = new ArrayList<>();
        private long nextFencingToken;
        private boolean deferClaims;
        private boolean deferModifications;
        private boolean deferFinishes;
        private boolean completeRenewals = true;
        private boolean failRenewalsWithLeaseLoss;
        private ActionListener<List<Tuple<String, Lease>>> pendingClaim;
        private PendingModification pendingModification;
        private ActionListener<String> pendingFinish;

        private TestTaskQueue(DeterministicTaskQueue deterministicTaskQueue) {
            this.deterministicTaskQueue = deterministicTaskQueue;
        }

        void add(String id, String state) {
            states.put(id, state);
            readyTasks.add(Map.entry(id, state));
        }

        @Override
        public void claim(String ownerId, int maxTasks, TimeValue leaseDuration, ActionListener<List<Tuple<String, Lease>>> listener) {
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
                    deterministicTaskQueue.getCurrentTimeMillis() + leaseDuration.millis()
                );
                result.add(new Tuple<>(states.get(task.getKey()), lease));
            }
            return result;
        }

        @Override
        public void renew(Lease lease, TimeValue leaseDuration, ActionListener<Lease> listener) {
            renewedLeases.add(lease);
            if (failRenewalsWithLeaseLoss) {
                listener.onFailure(new LeaseLostException("simulated fencing"));
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
        public void modify(Lease lease, String newState, ActionListener<String> listener) {
            if (deferModifications) {
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

        @Override
        public void finish(Lease lease, String finalState, ActionListener<String> listener) {
            if (deferFinishes) {
                assertNull(pendingFinish);
                pendingFinish = listener;
            } else {
                states.put(lease.taskId(), finalState);
                listener.onResponse(finalState);
            }
        }

        void failFinish() {
            var listener = pendingFinish;
            pendingFinish = null;
            listener.onFailure(new RuntimeException("simulated finish failure"));
        }

        @Override
        public void release(Lease lease, ActionListener<Void> listener) {
            releasedLeases.add(lease);
            listener.onResponse(null);
        }

        private record PendingModification(String taskId, String state, ActionListener<String> listener) {}
    }
}
