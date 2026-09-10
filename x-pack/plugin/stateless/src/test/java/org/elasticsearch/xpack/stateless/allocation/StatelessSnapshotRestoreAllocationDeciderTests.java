/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.ShardAndIndexHeapUsage;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.TestRoutingAllocationFactory;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;

/** Exercises restore admission through desired-balance simulation and reconciliation. */
public class StatelessSnapshotRestoreAllocationDeciderTests extends ESAllocationTestCase {
    private static final long GB = ByteSizeValue.ofGb(1).getBytes();
    private static final String NODE = "index-node";
    private static final String PATH = "/data";
    private final StatelessSnapshotRestoreAllocationDecider decider = new StatelessSnapshotRestoreAllocationDecider();

    private ClusterState state(int count) {
        var metadata = Metadata.builder();
        var routing = RoutingTable.builder(new StatelessShardRoutingRoleStrategy());
        for (int i = 0; i < count; i++) {
            var index = IndexMetadata.builder("index-" + i)
                .settings(settings(IndexVersion.current()).put("index.allocation.existing_shards_allocator", "stateless"))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();
            metadata.put(index, false);
            routing.addAsNewRestore(
                index,
                new RecoverySource.SnapshotRecoverySource(
                    RecoverySource.SnapshotRecoverySource.NO_API_RESTORE_UUID,
                    new Snapshot("repo", new SnapshotId("snapshot-" + i, "uuid-" + i)),
                    IndexVersion.current(),
                    new IndexId(index.getIndex().getName(), "repository-index-" + i)
                ),
                Set.of()
            );
        }
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routing.build())
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode(NODE, Set.of(DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.MASTER_ROLE)))
                    .localNodeId(NODE)
                    .masterNodeId(NODE)
            )
            .build();
    }

    private SnapshotShardSizeInfo sizes(ClusterState state, long size) {
        Map<InternalSnapshotsInfoService.SnapshotShard, Long> sizes = new HashMap<>();
        state.routingTable().allShards().forEach(shard -> {
            var source = (RecoverySource.SnapshotRecoverySource) shard.recoverySource();
            sizes.put(new InternalSnapshotsInfoService.SnapshotShard(source.snapshot(), source.index(), shard.shardId()), size);
        });
        return new SnapshotShardSizeInfo(sizes);
    }

    private ClusterInfo info(long free) {
        return info(Map.of(NODE, new DiskUsage(NODE, NODE, PATH, 200 * GB, free)), Map.of());
    }

    private ClusterInfo info(Map<String, DiskUsage> disks, Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservations) {
        return new ClusterInfo(
            disks,
            disks,
            Map.of(),
            Map.of(),
            Map.of(),
            reservations,
            Map.of(),
            Map.of(),
            ShardAndIndexHeapUsage.ZERO,
            Map.of(),
            Map.of(),
            Map.of(),
            Set.of(),
            Map.of(),
            Map.of(),
            Map.of()
        );
    }

    private Decision decide(ClusterState state, ClusterInfo info, SnapshotShardSizeInfo sizes) {
        var allocation = TestRoutingAllocationFactory.forClusterState(state).clusterInfo(info).shardSizeInfo(sizes).build();
        allocation.setDebugMode(RoutingAllocation.DebugMode.ON);
        return decider.canAllocate(
            state.getRoutingNodes().unassigned().iterator().next(),
            allocation.routingNodes().node(NODE),
            allocation
        );
    }

    public void testUnknownInformationAndHeadroomBoundary() {
        var state = state(1);
        assertEquals(Decision.Type.THROTTLE, decide(state, info(100 * GB), SnapshotShardSizeInfo.EMPTY).type());
        assertEquals(Decision.Type.THROTTLE, decide(state, info(100 * GB), sizes(state, -1)).type());
        assertEquals(Decision.Type.THROTTLE, decide(state, ClusterInfo.EMPTY, sizes(state, 50 * GB)).type());
        assertEquals(Decision.Type.YES, decide(state, info(55 * GB), sizes(state, 50 * GB)).type());
        var denied = decide(state, info(55 * GB - 1), sizes(state, 50 * GB));
        assertEquals(Decision.Type.THROTTLE, denied.type());
        assertThat(denied.getExplanation(), containsString("headroom [" + 5 * GB + "]"));
    }

    public void testOrdinaryAllocationsAndInitializingRestoreAreUnaffected() {
        var state = state(1);
        var allocation = TestRoutingAllocationFactory.forClusterState(state).clusterInfo(info(0)).build();
        var node = allocation.routingNodes().node(NODE);
        for (boolean primary : List.of(true, false)) {
            var shard = TestShardRouting.shardRoutingBuilder(
                state.routingTable().index("index-0").shard(0).primaryShard().shardId(),
                null,
                primary,
                ShardRoutingState.UNASSIGNED
            )
                .withRecoverySource(primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE)
                .withRole(primary ? ShardRouting.Role.INDEX_ONLY : ShardRouting.Role.SEARCH_ONLY)
                .build();
            assertEquals(Decision.Type.YES, decider.canAllocate(shard, node, allocation).type());
        }
        var initializing = state.routingTable().index("index-0").shard(0).primaryShard().initialize(NODE, null, 60 * GB);
        assertEquals(Decision.Type.YES, decider.canAllocate(initializing, node, allocation).type());
        var searchNode = ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder(state.nodes()).add(newNode("search", Set.of(DiscoveryNodeRole.SEARCH_ROLE))))
            .build()
            .getRoutingNodes()
            .node("search");
        assertEquals(
            Decision.Type.YES,
            decider.canAllocate(state.routingTable().index("index-0").shard(0).primaryShard(), searchNode, allocation).type()
        );
    }

    public void testAllocationWaitsForSnapshotSizeThenProceeds() {
        var state = state(1);
        var sizeInfo = new AtomicReference<>(SnapshotShardSizeInfo.EMPTY);
        var info = new AtomicReference<>(info(100 * GB));
        var service = service(info, sizeInfo);
        state = service.reroute(state, "size unavailable", ActionListener.noop());
        assertEquals(1, state.getRoutingNodes().unassigned().size());
        sizeInfo.set(sizes(state, 60 * GB));
        state = service.reroute(state, "snapshot size arrived", ActionListener.noop());
        assertTrue(state.routingTable().index("index-0").shard(0).primaryShard().initializing());
    }

    public void testReservationChangesTriggerMonitorButInitializingRestoresDoNot() {
        var state = new AtomicReference<>(state(1));
        var reroutes = new AtomicInteger();
        var monitor = new StatelessSnapshotRestoreStorageMonitor(state::get, (reason, priority, listener) -> {
            reroutes.incrementAndGet();
            listener.onResponse(null);
        });
        var disk = Map.of(NODE, new DiskUsage(NODE, NODE, PATH, 200 * GB, 100 * GB));
        var shardId = state.get().routingTable().index("index-0").shard(0).primaryShard().shardId();
        monitor.onNewInfo(
            info(disk, Map.of(new ClusterInfo.NodeAndPath(NODE, PATH), new ClusterInfo.ReservedSpace(70 * GB, Set.of(shardId))))
        );
        monitor.onNewInfo(
            info(disk, Map.of(new ClusterInfo.NodeAndPath(NODE, PATH), new ClusterInfo.ReservedSpace(20 * GB, Set.of(shardId))))
        );
        assertEquals(2, reroutes.get());
        var nodes = state.get().mutableRoutingNodes();
        var iterator = nodes.unassigned().iterator();
        iterator.next();
        iterator.initialize(NODE, null, 60 * GB, RoutingChangesObserver.NOOP);
        state.set(
            ClusterState.builder(state.get()).routingTable(state.get().globalRoutingTable().rebuild(nodes, state.get().metadata())).build()
        );
        monitor.onNewInfo(info(150 * GB));
        assertEquals(2, reroutes.get());
    }

    private AllocationService service(AtomicReference<ClusterInfo> info, AtomicReference<SnapshotShardSizeInfo> sizes) {
        var service = new AllocationService(
            new AllocationDeciders(List.of(decider, new StatelessAllocationDecider())),
            createShardsAllocator(Settings.builder().put("cluster.routing.allocation.type", "desired_balance").build()),
            info::get,
            sizes::get,
            new StatelessShardRoutingRoleStrategy(),
            MeterRegistry.NOOP
        );
        service.setExistingShardsAllocators(Map.of("stateless", new StatelessExistingShardsAllocator()));
        return service;
    }

    public void testConcurrentRestoresAccountForAssignmentsAndReportedReservations() {
        var state = state(2);
        var sizes = new AtomicReference<>(sizes(state, 60 * GB));
        var info = new AtomicReference<>(info(100 * GB));
        var service = service(info, sizes);
        state = service.reroute(state, "initial restores", ActionListener.noop());
        assertEquals(1, state.routingTable().allShards().filter(shard -> shard.initializing()).toList().size());
        assertEquals(1, state.getRoutingNodes().unassigned().size());
        state = service.reroute(state, "unchanged stats", ActionListener.noop());
        assertEquals(1, state.getRoutingNodes().unassigned().size());
        var incoming = state.routingTable().allShards().filter(shard -> shard.initializing()).toList().getFirst();
        var reservations = Map.of(
            new ClusterInfo.NodeAndPath(NODE, PATH),
            new ClusterInfo.ReservedSpace(40 * GB, Set.of(incoming.shardId()))
        );
        info.set(info(Map.of(NODE, new DiskUsage(NODE, NODE, PATH, 200 * GB, 80 * GB)), reservations));
        assertEquals(Decision.Type.THROTTLE, decide(state, info.get(), sizes.get()).type());
        // A 35 GiB candidate fits exactly after the 40 GiB reservation. Charging the full incoming shard again would reject it.
        var candidateSizes = sizes(state, 35 * GB);
        assertEquals(Decision.Type.YES, decide(state, info.get(), candidateSizes).type());
        info.set(info(150 * GB));
        state = service.reroute(state, "capacity added", ActionListener.noop());
        assertEquals(0, state.getRoutingNodes().unassigned().size());
    }

    public void testOutgoingShardDoesNotReleaseSpaceBeforeDeletion() {
        var state = state(2);
        var snapshotSizes = sizes(state, 60 * GB);
        state = ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder(state.nodes()).add(newNode("destination", Set.of(DiscoveryNodeRole.INDEX_ROLE))))
            .build();
        var nodes = state.mutableRoutingNodes();
        var iterator = nodes.unassigned().iterator();
        iterator.next();
        var incoming = iterator.initialize(NODE, null, 60 * GB, RoutingChangesObserver.NOOP);
        var started = nodes.startShard(incoming, RoutingChangesObserver.NOOP, 60 * GB);
        var source = nodes.relocateShard(
            started,
            "destination",
            60 * GB,
            "test relocation",
            RoutingChangesObserver.NOOP,
            ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO
        ).v1();
        state = ClusterState.builder(state).routingTable(state.globalRoutingTable().rebuild(nodes, state.metadata())).build();
        var disks = Map.of(NODE, new DiskUsage(NODE, NODE, PATH, 200 * GB, 40 * GB));
        var info = new ClusterInfo(
            disks,
            disks,
            Map.of(ClusterInfo.shardIdentifierFromRouting(source), 60 * GB),
            Map.of(),
            Map.of(ClusterInfo.NodeAndShard.from(source), PATH),
            Map.of(),
            Map.of(),
            Map.of(),
            ShardAndIndexHeapUsage.ZERO,
            Map.of(),
            Map.of(),
            Map.of(),
            Set.of(),
            Map.of(),
            Map.of(),
            Map.of()
        );
        assertEquals(Decision.Type.THROTTLE, decide(state, info, snapshotSizes).type());
    }

    public void testMonitorRetriesAllocationOnCapacityAndNewNodeInformation() {
        var state = new AtomicReference<>(state(1));
        var sizes = new AtomicReference<>(sizes(state.get(), 60 * GB));
        var info = new AtomicReference<>(info(40 * GB));
        var service = service(info, sizes);
        AtomicInteger reroutes = new AtomicInteger();
        var monitor = new StatelessSnapshotRestoreStorageMonitor(state::get, (reason, priority, listener) -> {
            reroutes.incrementAndGet();
            state.set(service.reroute(state.get(), reason, listener));
        });
        monitor.onNewInfo(info.get());
        assertEquals(1, state.get().getRoutingNodes().unassigned().size());
        monitor.onNewInfo(info.get());
        assertEquals(1, reroutes.get());
        var newNode = newNode("new-node", Set.of(DiscoveryNodeRole.INDEX_ROLE));
        state.set(ClusterState.builder(state.get()).nodes(DiscoveryNodes.builder(state.get().nodes()).add(newNode)).build());
        state.set(service.reroute(state.get(), "node joined without stats", ActionListener.noop()));
        assertEquals(1, state.get().getRoutingNodes().unassigned().size());
        info.set(
            info(
                Map.of(
                    NODE,
                    new DiskUsage(NODE, NODE, PATH, 200 * GB, 40 * GB),
                    "new-node",
                    new DiskUsage("new-node", "new-node", PATH, 200 * GB, 100 * GB)
                ),
                Map.of()
            )
        );
        monitor.onNewInfo(info.get());
        assertEquals("new-node", state.get().routingTable().index("index-0").shard(0).primaryShard().currentNodeId());
        // Node loss is handled through the allocation service, not by manually rebuilding the unassigned primary.
        state.set(ClusterState.builder(state.get()).nodes(DiscoveryNodes.builder(state.get().nodes()).remove("new-node")).build());
        state.set(service.disassociateDeadNodes(state.get(), true, "lost recovery node"));
        assertEquals(1, state.get().getRoutingNodes().unassigned().size());
        info.set(info(100 * GB));
        monitor.onNewInfo(info.get());
        assertEquals(NODE, state.get().routingTable().index("index-0").shard(0).primaryShard().currentNodeId());
    }

    public void testMonitorIgnoresSearchStorageAndStopsWithoutWaitingPrimaries() {
        var initialState = state(1);
        var state = new AtomicReference<>(
            ClusterState.builder(initialState)
                .nodes(DiscoveryNodes.builder(initialState.nodes()).add(newNode("search-node", Set.of(DiscoveryNodeRole.SEARCH_ROLE))))
                .build()
        );
        AtomicInteger reroutes = new AtomicInteger();
        var monitor = new StatelessSnapshotRestoreStorageMonitor(state::get, (reason, priority, listener) -> {
            reroutes.incrementAndGet();
            listener.onResponse(null);
        });
        monitor.onNewInfo(info(100 * GB));
        monitor.onNewInfo(
            info(
                Map.of(
                    NODE,
                    new DiskUsage(NODE, NODE, PATH, 200 * GB, 100 * GB),
                    "search-node",
                    new DiskUsage("search-node", "search-node", PATH, 200 * GB, 10 * GB)
                ),
                Map.of()
            )
        );
        assertEquals(1, reroutes.get());
        var disks = Map.of(NODE, new DiskUsage(NODE, NODE, PATH, 200 * GB, 100 * GB));
        monitor.onNewInfo(
            new ClusterInfo(
                disks,
                disks,
                Map.of("unrelated-shard", 123L),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                ShardAndIndexHeapUsage.ZERO,
                Map.of(),
                Map.of(),
                Map.of(),
                Set.of(),
                Map.of(),
                Map.of(),
                Map.of()
            )
        );
        assertEquals(1, reroutes.get());
        state.set(state(0));
        monitor.onNewInfo(info(150 * GB));
        assertEquals(1, reroutes.get());
        state.set(ClusterState.builder(state(1)).nodes(DiscoveryNodes.builder(state.get().nodes()).masterNodeId(null)).build());
        monitor.onNewInfo(info(160 * GB));
        assertEquals(1, reroutes.get());
    }
}
