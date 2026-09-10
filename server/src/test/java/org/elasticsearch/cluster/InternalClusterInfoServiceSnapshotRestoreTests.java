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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;

import java.util.List;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Exercises the extra collection demand from snapshot restores, independently of allocation and recovery. */
public class InternalClusterInfoServiceSnapshotRestoreTests extends ESTestCase {
    public void testRestoreActivationDuringRefreshQueuesStoreCollection() {
        var fixture = new Fixture(CollectionMode.HEAP);
        fixture.startRefresh();

        fixture.setRestore(UNASSIGNED);
        assertEquals(0, fixture.storeRequests);

        fixture.completeRefresh();
        assertEquals(1, fixture.storeRequests);
    }

    public void testRestoreStatesRequiringStats() {
        record CollectionCase(String description, ShardRoutingState restoreState, int expectedRequests) {}

        var cases = List.of(
            new CollectionCase("no restore", null, 0),
            new CollectionCase("waiting", UNASSIGNED, 1),
            new CollectionCase("recovering", INITIALIZING, 1),
            new CollectionCase("completed", STARTED, 0)
        );
        for (var testCase : cases) {
            var fixture = new Fixture(CollectionMode.NONE);
            fixture.setRestore(testCase.restoreState());
            assertEquals(testCase.description(), testCase.expectedRequests(), fixture.storeRequests);
            assertEquals(testCase.description(), testCase.expectedRequests(), fixture.nodeStatsRequests);
            fixture.completeRefresh();
        }
    }

    public void testRemovingRestoreDemandStopsCollection() {
        var fixture = new Fixture(CollectionMode.NONE);
        assertEquals(0, fixture.storeRequests);
        assertEquals(0, fixture.nodeStatsRequests);

        fixture.setRestore(UNASSIGNED);
        fixture.completeRefresh();
        assertEquals(1, fixture.storeRequests);
        assertEquals(1, fixture.nodeStatsRequests);

        fixture.setRestore(null);
        fixture.periodicRefresh();
        assertEquals(1, fixture.storeRequests);
        assertEquals(1, fixture.nodeStatsRequests);
    }

    public void testRemovingRestoreDemandPreservesDiskCollection() {
        var fixture = new Fixture(CollectionMode.DISK);
        assertEquals(1, fixture.storeRequests);
        assertEquals(1, fixture.nodeStatsRequests);

        fixture.setRestore(UNASSIGNED);
        fixture.completeRefresh();
        assertEquals(2, fixture.storeRequests);
        assertEquals(2, fixture.nodeStatsRequests);

        fixture.setRestore(null);
        fixture.periodicRefresh();
        assertEquals(3, fixture.storeRequests);
        assertEquals(3, fixture.nodeStatsRequests);
    }

    private enum CollectionMode {
        NONE,
        HEAP,
        DISK
    }

    private class Fixture {
        final DeterministicTaskQueue queue = new DeterministicTaskQueue();
        final InternalClusterInfoService service;
        final DiscoveryNode node = DiscoveryNodeUtils.create("node");
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(node).localNodeId(node.getId()))
            .build();
        int storeRequests;
        int nodeStatsRequests;

        Fixture(CollectionMode collectionMode) {
            var settings = Settings.builder()
                .put("stateless.enabled", true)
                .put(
                    DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(),
                    collectionMode == CollectionMode.DISK
                )
                .put(
                    WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                    WriteLoadConstraintSettings.WriteLoadDeciderStatus.DISABLED
                )
                .put(
                    InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED.getKey(),
                    collectionMode == CollectionMode.HEAP
                )
                .build();
            var clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            // Only settings and state are needed; avoid starting cluster-service executors.
            var clusterService = mock(ClusterService.class);
            when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
            when(clusterService.state()).thenAnswer(invocation -> state);
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
                    // Only request scheduling matters here; fail requests to finish the refresh without constructing stats.
                    queue.scheduleNow(() -> listener.onFailure(new Exception("stats values are irrelevant to this test")));
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
        void setRestore(ShardRoutingState restoreState) {
            var metadata = Metadata.builder();
            var routing = RoutingTable.builder();
            if (restoreState != null) {
                var index = IndexMetadata.builder("index")
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build();
                metadata.put(index, false);
                var shard = TestShardRouting.shardRoutingBuilder(
                    new ShardId(index.getIndex(), 0),
                    restoreState == UNASSIGNED ? null : node.getId(),
                    true,
                    restoreState
                );
                if (restoreState != STARTED) {
                    shard.withRecoverySource(
                        new RecoverySource.SnapshotRecoverySource(
                            RecoverySource.SnapshotRecoverySource.NO_API_RESTORE_UUID,
                            new Snapshot("repo", new SnapshotId("snap", "uuid")),
                            IndexVersion.current(),
                            new IndexId("index", "id")
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

    }
}
