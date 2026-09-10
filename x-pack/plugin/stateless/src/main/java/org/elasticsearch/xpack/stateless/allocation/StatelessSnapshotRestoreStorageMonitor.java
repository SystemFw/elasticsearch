/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/** Requests reroutes for pending snapshot restores when indexing-node storage information changes. */
public class StatelessSnapshotRestoreStorageMonitor {
    private static final Logger logger = LogManager.getLogger(StatelessSnapshotRestoreStorageMonitor.class);
    private final Supplier<ClusterState> clusterState;
    private final RerouteService rerouteService;
    // Accessed only by onNewInfo callbacks. InternalClusterInfoService serializes these callbacks and
    // safely publishes their writes through the synchronized refresh handoff, even when the callback thread changes.
    private Map<String, Storage> previous = Map.of();

    private record Storage(String path, long freeBytes, ClusterInfo.ReservedSpace reservations) {}

    public StatelessSnapshotRestoreStorageMonitor(Supplier<ClusterState> clusterState, RerouteService rerouteService) {
        this.clusterState = clusterState;
        this.rerouteService = rerouteService;
    }

    public void onNewInfo(ClusterInfo info) {
        var state = clusterState.get();
        if (state.nodes().isLocalNodeElectedMaster() == false) {
            previous = Map.of();
            return;
        }
        Map<String, Storage> current = new HashMap<>();
        for (var node : state.nodes()) {
            if (node.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE)) {
                var disk = info.getNodeMostAvailableDiskUsages().get(node.getId());
                if (disk != null) {
                    current.put(node.getId(), new Storage(disk.path(), disk.freeBytes(), info.getReservedSpace(node.getId(), disk.path())));
                }
            }
        }
        boolean changed = current.equals(previous) == false;
        previous = Collections.unmodifiableMap(current);
        if (changed
            && state.getRoutingNodes()
                .unassigned()
                .stream()
                .anyMatch(shard -> shard.primary() && shard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT)) {
            rerouteService.reroute(
                "snapshot restore storage updated",
                Priority.HIGH,
                ActionListener.wrap(ignored -> {}, e -> logger.debug("reroute after snapshot restore storage update failed", e))
            );
        }
    }
}
