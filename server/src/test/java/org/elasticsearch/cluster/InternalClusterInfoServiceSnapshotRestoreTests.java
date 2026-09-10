/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Exercises restore-driven collection with controlled asynchronous request completion. */
public class InternalClusterInfoServiceSnapshotRestoreTests extends ESTestCase {
    public void testRestoreActivationQueuesRefreshBehindInFlightRequests() throws Exception {
        try (var fixture = new Fixture(true, false)) {
            fixture.apply(0, false, true);
            assertEquals(1, fixture.client.pending.size()); // heap monitoring requests node stats
            fixture.apply(1, false, true);
            assertEquals(1, fixture.client.pending.size()); // no concurrent refresh
            assertTrue(fixture.client.storeRequests.isEmpty());
            fixture.completeRequests();
            assertEquals(List.of(true), fixture.client.storeRequests);
            assertEquals(2, fixture.client.pending.size()); // follow-up store and node requests
            fixture.completeRequests();
        }
    }

    public void testOverlappingRestoresInitializationCancellationAndMasterElection() throws Exception {
        try (var fixture = new Fixture(true, false)) {
            fixture.apply(1, false, true);
            fixture.completeRequests();
            assertEquals(List.of(true), fixture.client.storeRequests);
            fixture.apply(2, false, true);
            assertTrue(fixture.client.pending.isEmpty()); // overlap does not request another immediate refresh
            fixture.periodicRefresh();
            assertEquals(List.of(true, true), fixture.client.storeRequests);
            fixture.apply(1, true, true); // one restore remains, now initializing
            fixture.periodicRefresh();
            assertEquals(List.of(true, true, true), fixture.client.storeRequests);
            fixture.startPrimaries(); // completing the last restore removes collection demand without deleting the index
            fixture.periodicRefresh();
            assertEquals(3, fixture.client.storeRequests.size());
            fixture.apply(1, false, false); // restore exists, but this node is not master
            assertTrue(fixture.client.pending.isEmpty());
            fixture.apply(1, false, true); // election must rediscover the pending restore
            assertFalse(fixture.client.pending.isEmpty());
            fixture.completeRequests();
            assertEquals(4, fixture.client.storeRequests.size());
        }
    }

    public void testStatefulRestoreDoesNotEnableStoreCollection() throws Exception {
        try (var fixture = new Fixture(false, false)) {
            fixture.apply(1, false, true);
            fixture.completeRequests();
            assertTrue(fixture.client.storeRequests.isEmpty());
        }
    }

    public void testExistingDiskConsumerKeepsStoreCollectionEnabled() throws Exception {
        try (var fixture = new Fixture(true, true)) {
            fixture.apply(1, false, true);
            fixture.completeRequests();
            fixture.apply(0, false, true);
            fixture.periodicRefresh();
            assertEquals(List.of(true, true), fixture.client.storeRequests);
        }
    }

    public void testRestoreCollectsFilesystemStatsWithoutHeapOrDiskConsumers() throws Exception {
        try (var fixture = new Fixture(true, false, false)) {
            fixture.apply(0, false, true);
            assertTrue(fixture.client.pending.isEmpty());
            fixture.apply(1, false, true);
            assertEquals(List.of(true), fixture.client.storeRequests);
            assertEquals(2, fixture.client.pending.size());
            fixture.completeRequests();
        }
    }

    public void testCancellationAndMasterLossStopExtraCollection() throws Exception {
        try (var fixture = new Fixture(true, false)) {
            fixture.apply(1, false, true);
            fixture.completeRequests();
            fixture.apply(0, false, true);
            fixture.periodicRefresh();
            assertEquals(List.of(true), fixture.client.storeRequests);
            fixture.apply(1, false, true);
            fixture.completeRequests();
            fixture.apply(1, false, false);
            fixture.queue.advanceTime();
            fixture.queue.runAllRunnableTasks();
            assertTrue(fixture.client.pending.isEmpty());
            fixture.apply(1, false, true);
            fixture.completeRequests();
            assertEquals(List.of(true, true, true), fixture.client.storeRequests);
        }
    }

    private class Fixture implements AutoCloseable {
        final DeterministicTaskQueue queue = new DeterministicTaskQueue();
        final ClusterService clusterService;
        final RecordingClient client;
        final InternalClusterInfoService service;
        final DiscoveryNode node = DiscoveryNodeUtils.create("node");
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(node).localNodeId(node.getId()))
            .build();

        Fixture(boolean stateless, boolean diskEnabled) {
            this(stateless, diskEnabled, true);
        }

        Fixture(boolean stateless, boolean diskEnabled, boolean heapEnabled) {
            var settings = Settings.builder()
                .put("stateless.enabled", stateless)
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), diskEnabled)
                .put(
                    WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                    WriteLoadConstraintSettings.WriteLoadDeciderStatus.DISABLED
                )
                .put(InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED.getKey(), heapEnabled)
                .build();
            var clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            clusterService = ClusterServiceUtils.createClusterService(queue.getThreadPool(), clusterSettings);
            client = new RecordingClient(queue.getThreadPool());
            service = new InternalClusterInfoService(
                settings,
                new WriteLoadConstraintSettings(clusterSettings),
                clusterService,
                queue.getThreadPool(),
                client,
                EstimatedHeapUsageCollector.EMPTY,
                CacheSizesAndCommitmentCollector.EMPTY,
                PartitionSizeCollector.EMPTY,
                NodeUsageStatsForThreadPoolsCollector.EMPTY
            );
            service.addListener(ignored -> {});
        }

        void apply(int count, boolean initializing, boolean master) {
            var metadata = Metadata.builder();
            var routing = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
            for (int i = 0; i < count; i++) {
                var index = IndexMetadata.builder("index-" + i)
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build();
                metadata.put(index, false);
                routing.addAsNewRestore(
                    index,
                    new RecoverySource.SnapshotRecoverySource(
                        RecoverySource.SnapshotRecoverySource.NO_API_RESTORE_UUID,
                        new Snapshot("repo", new SnapshotId("snap", "uuid")),
                        IndexVersion.current(),
                        new IndexId("index-" + i, "id-" + i)
                    ),
                    Set.of()
                );
            }
            var next = ClusterState.builder(ClusterName.DEFAULT)
                .metadata(metadata)
                .routingTable(routing.build())
                .nodes(DiscoveryNodes.builder().add(node).localNodeId(node.getId()).masterNodeId(master ? node.getId() : null))
                .build();
            if (initializing) {
                var nodes = next.mutableRoutingNodes();
                var iterator = nodes.unassigned().iterator();
                while (iterator.hasNext()) {
                    iterator.next();
                    iterator.initialize(node.getId(), null, 100L, RoutingChangesObserver.NOOP);
                }
                next = ClusterState.builder(next).routingTable(next.globalRoutingTable().rebuild(nodes, next.metadata())).build();
            }
            service.clusterChanged(new ClusterChangedEvent("test", next, state));
            state = next;
        }

        void startPrimaries() {
            var nodes = state.mutableRoutingNodes();
            state.routingTable().allShards().forEach(shard -> nodes.startShard(shard, RoutingChangesObserver.NOOP, 100L));
            var next = ClusterState.builder(state).routingTable(state.globalRoutingTable().rebuild(nodes, state.metadata())).build();
            service.clusterChanged(new ClusterChangedEvent("restore completed", next, state));
            state = next;
        }

        void completeRequests() {
            var pending = List.copyOf(client.pending);
            client.pending.clear();
            pending.forEach(Runnable::run);
            queue.runAllRunnableTasks();
        }

        void periodicRefresh() {
            assertTrue(client.pending.isEmpty());
            queue.advanceTime();
            queue.runAllRunnableTasks();
            completeRequests();
        }

        @Override
        public void close() {
            apply(0, false, false);
            while (client.pending.isEmpty() == false) {
                completeRequests();
            }
            clusterService.close();
        }
    }

    // Hold transport completions to control refresh overlap. A quiet failure is sufficient here: these tests inspect
    // request selection and scheduling, while allocation/integration tests exercise successful metric publication.
    private static class RecordingClient extends NoOpClient {
        final List<Boolean> storeRequests = new ArrayList<>();
        final List<Runnable> pending = new ArrayList<>();

        RecordingClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof IndicesStatsRequest indices) {
                storeRequests.add(indices.store());
            } else {
                assertTrue(request instanceof NodesStatsRequest);
            }
            pending.add(() -> listener.onFailure(new ClusterBlockException(Set.of(NoMasterBlockService.NO_MASTER_BLOCK_ALL))));
        }
    }
}
