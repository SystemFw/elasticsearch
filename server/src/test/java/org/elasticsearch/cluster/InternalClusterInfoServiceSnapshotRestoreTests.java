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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;

/** Exercises the extra collection demand from snapshot restores, independently of allocation and recovery. */
public class InternalClusterInfoServiceSnapshotRestoreTests extends ESTestCase {
    public void testRestoreActivationDuringRefreshQueuesStoreCollection() {
        try (var fixture = new Fixture(Settings.EMPTY)) {
            assertEquals(1, fixture.pending.size()); // the existing heap consumer requests node stats

            fixture.setRestores(UNASSIGNED);
            assertEquals(1, fixture.pending.size()); // still the original refresh
            assertEquals(0, fixture.storeRequests);

            fixture.completeRefresh();
            assertEquals(1, fixture.storeRequests);
            assertEquals(2, fixture.pending.size()); // queued refresh includes store and node stats
            fixture.completeRefresh();
        }
    }

    public void testRestoreStatesRequiringStoreStats() {
        record CollectionCase(String description, List<ShardRoutingState> restoreStates, boolean expectStoreStats) {}

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
            try (var fixture = new Fixture(Settings.EMPTY)) {
                fixture.completeRefresh();
                fixture.setRestores(testCase.restoreStates().toArray(ShardRoutingState[]::new));
                assertEquals(testCase.description(), testCase.expectStoreStats(), fixture.storeRequests > 0);
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
                fixture.completeRefresh();
                int initialStoreRequests = fixture.storeRequests;
                fixture.setRestores(UNASSIGNED);
                fixture.completeRefresh();
                assertEquals(initialStoreRequests + 1, fixture.storeRequests);

                fixture.setRestores();
                fixture.periodicRefresh();
                assertEquals(initialStoreRequests + (diskEnabled ? 2 : 1), fixture.storeRequests);
            }
        }
    }

    private class Fixture implements AutoCloseable {
        final DeterministicTaskQueue queue = new DeterministicTaskQueue();
        final List<Runnable> pending = new ArrayList<>();
        final ClusterService clusterService;
        final InternalClusterInfoService service;
        final DiscoveryNode node = DiscoveryNodeUtils.create("node");
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(node).localNodeId(node.getId()))
            .build();
        int storeRequests;

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
            // As in the scheduling tests, quiet request failures suffice to test collection rather than metric values.
            // Hold their completion here so a restore can begin midway through a refresh.
            var client = new NoOpClient(queue.getThreadPool()) {
                @Override
                protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    if (request instanceof IndicesStatsRequest indices) {
                        assertTrue(indices.store());
                        storeRequests++;
                    } else {
                        assertTrue(request instanceof NodesStatsRequest);
                    }
                    pending.add(() -> listener.onFailure(new ClusterBlockException(Set.of(NoMasterBlockService.NO_MASTER_BLOCK_ALL))));
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

        void completeRefresh() {
            var completing = List.copyOf(pending);
            pending.clear(); // callbacks may enqueue the next refresh's requests
            completing.forEach(Runnable::run);
            queue.runAllRunnableTasks();
        }

        void periodicRefresh() {
            assertTrue(pending.isEmpty());
            queue.advanceTime();
            queue.runAllRunnableTasks();
            completeRefresh();
        }

        @Override
        public void close() {
            update(ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).masterNodeId(null)).build());
            while (pending.isEmpty() == false) {
                completeRefresh();
            }
            clusterService.close();
        }
    }
}
