/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.unit.ByteSizeValue;

/** Prevents snapshot restores from being admitted without space for their local files and a node-wide reserve. */
public class StatelessSnapshotRestoreAllocationDecider extends AllocationDecider {
    private static final String NAME = "stateless_snapshot_restore_storage";
    private static final long HEADROOM_BYTES = ByteSizeValue.ofGb(5).getBytes();

    @Override
    public Decision canAllocate(ShardRouting shard, RoutingNode node, RoutingAllocation allocation) {
        if (shard.primary() == false
            || shard.unassigned() == false
            || shard.recoverySource().getType() != RecoverySource.Type.SNAPSHOT
            || node.node().getRoles().contains(DiscoveryNodeRole.INDEX_ROLE) == false) {
            return Decision.YES;
        }
        Long size = allocation.snapshotShardSizeInfo().getShardSize(shard);
        if (size == null || size < 0) {
            return allocation.decision(Decision.THROTTLE, NAME, "snapshot shard size is unavailable");
        }
        var disk = allocation.clusterInfo().getNodeMostAvailableDiskUsages().get(node.nodeId());
        if (disk == null) {
            return allocation.decision(Decision.THROTTLE, NAME, "node disk information is unavailable");
        }
        // Include recoveries assigned since the last stats refresh; never credit outgoing files before deletion.
        long committed = DiskThresholdDecider.sizeOfUnaccountedShards(
            node,
            false,
            disk.path(),
            allocation.clusterInfo(),
            allocation.snapshotShardSizeInfo(),
            allocation.metadata(),
            allocation.globalRoutingTable(),
            allocation.unaccountedSearchableSnapshotSize(node)
        );
        long usable = disk.freeBytes() - committed;
        boolean fits = usable >= HEADROOM_BYTES && size <= usable - HEADROOM_BYTES;
        return allocation.decision(
            fits ? Decision.YES : Decision.NO,
            NAME,
            "snapshot restore storage: free [%d] bytes, incoming commitments [%d] bytes, shard [%d] bytes, headroom [%d] bytes",
            disk.freeBytes(),
            committed,
            size,
            HEADROOM_BYTES
        );
    }
}
