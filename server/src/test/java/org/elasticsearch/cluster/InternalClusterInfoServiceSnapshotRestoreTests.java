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
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;

/** Exercises the extra collection demand from snapshot restores, independently of allocation and recovery. */
public class InternalClusterInfoServiceSnapshotRestoreTests extends ESTestCase {
    public void testRestoreActivationDuringRefreshQueuesStoreCollection() {
        try (var fixture = new Fixture(Settings.EMPTY)) {
            fixture.startRefresh();

            fixture.setRestores(UNASSIGNED);
            assertEquals(0, fixture.storeRequests);

            fixture.completeRefresh();
            assertEquals(1, fixture.storeRequests);
        }
    }

    public void testRestoreStatesRequiringStats() {
        record CollectionCase(String description, List<ShardRoutingState> restoreStates, boolean expectStats) {}

        var cases = List.of(
            new CollectionCase("no restore", List.of(), false),
            new CollectionCase("waiting", List.of(UNASSIGNED), true),
            new CollectionCase("recovering", List.of(INITIALIZING), true),
            new CollectionCase("completed", List.of(STARTED), false),
            new CollectionCase("multiple waiting", List.of(UNASSIGNED, UNASSIGNED), true),
            new CollectionCase("one still recovering", List.of(STARTED, INITIALIZING), true),
            new CollectionCase("all completed", List.of(STARTED, STARTED), false)
        );
        for (var testCase : cases) {
            var overrides = Settings.builder()
                .put(InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED.getKey(), false)
                .build();
            try (var fixture = new Fixture(overrides)) {
                fixture.setRestores(testCase.restoreStates().toArray(ShardRoutingState[]::new));
                assertEquals(testCase.description(), testCase.expectStats() ? 1 : 0, fixture.storeRequests);
                assertEquals(testCase.description(), testCase.expectStats() ? 1 : 0, fixture.nodeStatsRequests);
                fixture.completeRefresh();
            }
        }
    }

    public void testRemovingRestoreDemandPreservesDiskCollection() {
        for (boolean diskEnabled : List.of(false, true)) {
            var overrides = Settings.builder()
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), diskEnabled)
                .put(InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED.getKey(), false)
                .build();
            try (var fixture = new Fixture(overrides)) {
                int initialStoreRequests = fixture.storeRequests;
                int initialNodeStatsRequests = fixture.nodeStatsRequests;
                if (diskEnabled == false) {
                    assertEquals(0, initialStoreRequests);
                    assertEquals(0, initialNodeStatsRequests);
                }
                fixture.setRestores(UNASSIGNED);
                fixture.completeRefresh();
                assertEquals(initialStoreRequests + 1, fixture.storeRequests);
                assertEquals(initialNodeStatsRequests + 1, fixture.nodeStatsRequests);

                fixture.setRestores();
                fixture.periodicRefresh();
                assertEquals(initialStoreRequests + (diskEnabled ? 2 : 1), fixture.storeRequests);
                assertEquals(initialNodeStatsRequests + (diskEnabled ? 2 : 1), fixture.nodeStatsRequests);
            }
        }
    }

    private class Fixture implements AutoCloseable {
        final DeterministicTaskQueue queue = new DeterministicTaskQueue();
        final ClusterService clusterService;
        final InternalClusterInfoService service;
        final DiscoveryNode node = DiscoveryNodeUtils.create("node");
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(node).localNodeId(node.getId()))
            .build();
        int storeRequests;
        int nodeStatsRequests;

        Fixture(Settings overrides) {
            var settings = Settings.builder()
                .put("stateless.enabled", true)
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
                .put(
                    WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                    WriteLoadConstraintSettings.WriteLoadDeciderStatus.DISABLED
                )
                .put(InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED.getKey(), true)
                .put(overrides)
                .build();
            var clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            clusterService = ClusterServiceUtils.createClusterService(queue.getThreadPool(), clusterSettings);
            // Hold responses so a restore can begin midway through a refresh.
            var client = new NoOpClient(queue.getThreadPool()) {
                @Override
                protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    if (request instanceof IndicesStatsRequest indices && indices.store()) {
                        storeRequests++;
                    } else if (request instanceof NodesStatsRequest) {
                        nodeStatsRequests++;
                    }
                    // As in InternalClusterInfoServiceSchedulingTests, finish without metrics using a failure the service handles quietly.
                    queue.scheduleNow(
                        () -> listener.onFailure(new ClusterBlockException(Set.of(NoMasterBlockService.NO_MASTER_BLOCK_ALL)))
                    );
                }
            };
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
            update(ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).masterNodeId(node.getId())).build());
            completeRefresh();
        }

        // Supply just the routing states the collector observes, without simulating a restore workflow.
        void setRestores(ShardRoutingState... states) {
            var metadata = Metadata.builder();
            var routing = RoutingTable.builder();
            for (int i = 0; i < states.length; i++) {
                var index = IndexMetadata.builder("index-" + i)
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build();
                metadata.put(index, false);
                var shard = TestShardRouting.shardRoutingBuilder(
                    new ShardId(index.getIndex(), 0),
                    states[i] == UNASSIGNED ? null : node.getId(),
                    true,
                    states[i]
                );
                if (states[i] != STARTED) {
                    shard.withRecoverySource(
                        new RecoverySource.SnapshotRecoverySource(
                            RecoverySource.SnapshotRecoverySource.NO_API_RESTORE_UUID,
                            new Snapshot("repo", new SnapshotId("snap", "uuid")),
                            IndexVersion.current(),
                            new IndexId("index-" + i, "id-" + i)
                        )
                    );
                }
                routing.add(IndexRoutingTable.builder(index.getIndex()).addShard(shard.build()));
            }
            update(ClusterState.builder(state).metadata(metadata).routingTable(routing.build()).build());
        }

        private void update(ClusterState next) {
            service.clusterChanged(new ClusterChangedEvent("test", next, state));
            state = next;
        }

        void startRefresh() {
            service.refreshAsync(ActionListener.noop());
        }

        void completeRefresh() {
            queue.runAllRunnableTasks();
        }

        void periodicRefresh() {
            queue.advanceTime();
            completeRefresh();
        }

        @Override
        public void close() {
            update(ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).masterNodeId(null)).build());
            completeRefresh();
            clusterService.close();
        }
    }
}
