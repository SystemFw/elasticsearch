# Stateless Snapshot Restore Plan

## Status and purpose

This document records the current plan for a stateless-specific snapshot restore process. It is intended to be detailed enough to resume
design and implementation work without reconstructing the decisions that led here. It is still a scoping design rather than a final
implementation specification.

The new process is separate from the recovery-based restore implemented by `RestoreService`. It may reuse repository readers, BCC
serialization, stateless recovery, and object-store clients, but it does not use `RestoreInProgress` or persistent tasks to track copying.

## Motivation

The existing restore process allocates restored primaries in `INITIALIZING` with a `SnapshotRecoverySource`. Each allocated shard downloads
all of its snapshot files to local disk before it can become `STARTED`. On a stateless node this has several undesirable consequences:

* Local disk becomes durable restore storage rather than a cache.
* Allocation must find a node with enough free disk for the complete shard.
* Autoscaling must account for temporary restore disk requirements.
* Restore data competes with the shared blob cache and ongoing indexing for disk.
* A shard remains unavailable for the duration of the copy.
* `RestoreInProgress` stores the restore and all per-shard terminal states in cluster state. Shard completions cause routing publications
  that also rewrite this full shard-status map.

The current stateless path ultimately uploads the recovered Lucene commit to the destination object store as a BCC. The proposed design
moves this conversion and upload before allocation, avoiding the complete local copy.

## Goals

* Copy snapshot data into the destination stateless object store before allocating restored shards.
* Use bounded local memory and disk independent of shard size.
* Let activated shards bootstrap through normal stateless object-store recovery and become `STARTED` quickly.
* Keep restore progress and the per-shard work queue out of cluster state.
* Recover coordination and worker ownership after graceful shutdown, node crashes, and coordinator crashes.
* Make every externally visible transition idempotent.
* Preserve historical restore results for status and diagnosis.
* Bound transfer concurrency and its effect on normal indexing, snapshotting, and search traffic.

## Non-goals

* Integration with `RestoreInProgress` or the existing snapshot recovery state machine.
* One persistent task per restore or per shard.
* Byte-level progress in cluster state.
* Strongly consistent or exactly-once scheduling. Duplicate transfer work is acceptable; publishing an incorrect shard is not.
* Requiring every provider to support server-side object copy. Streaming through a worker remains the portable path.

## Architecture overview

The process has four durable components:

1. A restore document in an internal managed system index.
2. One upload-task document per shard in an internal managed system index.
3. Immutable BCC data in the destination object store.
4. A small activation marker in `IndexMetadata`, written only during the final cluster-state transition.

Coordinator and worker services run on data nodes. They claim restore or task documents using leases implemented with atomic conditional
scripted updates. They do not depend on a service running exclusively on the elected master. The elected master is involved only when the
coordinator submits the final cluster-state update that creates or activates the destination index and its routing.

The internal indices use `SystemIndexDescriptor.Type.INTERNAL_MANAGED`, so Elasticsearch owns their mappings and settings and users do not
modify them through ordinary index APIs.

## Restore lifecycle

Suggested restore states are:

* `CREATING`: the restore intent exists, but the complete task set has not been sealed.
* `COPYING`: workers may claim shard upload tasks.
* `FINALIZING`: all required shard outputs exist and activation is being reconciled.
* `DONE`: activation was committed and the required shards reached the chosen completion condition.
* `FAILED`: the restore cannot make progress without explicit intervention.
* `CANCELLING` and `CANCELLED`: no new work or activation may begin.

Transitions are monotonic. Restore documents are updated atomically and include a schema version, timestamps, failure details, and cached
task counts for reporting. Cached counts are not used as the correctness condition for activation.

### Creating a restore

The start operation:

1. Resolves the repository and immutable snapshot UUID.
2. Resolves source indices, snapshot shard generations, rename rules, and restore options.
3. Generates a restore UUID and destination index UUIDs.
4. Creates the restore document using create-only semantics.
5. Creates deterministic task documents, one for each destination shard.
6. Records the expected task IDs or an equivalent plan digest and expected count.
7. Sets `tasks_sealed=true` and advances the restore to `COPYING`.

Destination index UUIDs are generated during planning and are used in destination object-store paths. The destination indices do not need
to exist in cluster state while copying. This avoids a preliminary cluster-state update.

Task IDs are derived from the restore UUID, destination index UUID, and shard number. Repeating planning after a crash therefore converges
on the same documents. Workers ignore tasks belonging to a restore whose task set is not sealed.

## Restore coordinator

Every eligible coordinator periodically searches for nonterminal restore documents. It claims a restore with a renewable lease and works
on one lifecycle transition at a time. If it stops, another coordinator takes over after lease expiry and reconstructs the operation from
the restore document, task documents, object store, and cluster state.

The lease reduces duplicate coordination but is not the correctness boundary. Coordinator operations must remain idempotent because a
stale coordinator may continue briefly after losing its lease.

During `COPYING`, the coordinator monitors terminal task outcomes. Before finalization it verifies the actual expected task set rather than
trusting cached counters. Every expected task must identify a complete, readable, and valid shard output.

During `FINALIZING`, the coordinator reconciles cluster state using the activation protocol below. After activation it waits for the chosen
completion condition, normally all restored primaries reaching `STARTED`, and then marks the restore `DONE`.

## Upload task queue

Each shard task records at least:

* Restore UUID and plan version.
* Source repository UUID, snapshot UUID, source index ID, snapshot shard generation, and shard number.
* Destination project, index UUID, shard number, and object prefix.
* Task-specific state, terminal outcome, attempt count, and retry time.
* Lease owner, random lease ID, and expiry.
* Source-file plan or a digest of that plan.
* Upload identity and provider-specific resumability information where necessary.
* Logical bytes, transferred bytes, and timestamps for observability.
* Published BCC objects, checksums, format version, and terminal failure information.

Queue lifecycle is represented by terminality and an optional lease rather than by a shared task-state enum. A nonterminal task is
available when it has no lease or its lease has expired, subject to any retry time in its task-specific state. A nonterminal task with a
live lease and every terminal task are unavailable. The final task-specific state records whether the terminal outcome is success or
failure.

Workers find available tasks through searches. Claiming, renewing, reclaiming, modifying, releasing, and finishing a task uses an atomic
scripted update that checks terminality, lease identity, and lease expiry. Finishing atomically stores the final state, marks the task
terminal, and removes its lease. Native `_seq_no`/`_primary_term` optimistic concurrency control may additionally be used when claiming a
document returned by a search, but field-based lease checks are implemented by scripts executing atomically on the primary shard.

Document-level fencing does not fence object-store writes. Each attempt must therefore write immutable content-addressed objects or use a
lease-specific staging prefix. Only the current lease holder can make its uploaded objects authoritative by successfully publishing their
references to the task document. Objects produced by a stale worker remain unreferenced and are later garbage-collected.

Progress and heartbeat writes are rate-limited. Scheduling guarantees may be weak and work may be duplicated, but a stale worker must not
replace an authoritative task result.

## Snapshot-to-BCC conversion

A snapshot shard is logically a Lucene commit represented by individual repository blobs plus shard snapshot metadata. Large Lucene files
may be split into repository chunks, and unchanged files may be deduplicated across snapshots. The repository does not store a BCC.

Conversion consists of:

1. Reading the shard snapshot manifest and resolving every Lucene file, chunk, length, and checksum.
2. Producing the destination commit metadata required by stateless recovery, including the appropriate sequence-number, history,
   primary-term, and translog recovery fields.
3. Producing `StatelessCompoundCommit` metadata that maps Lucene files to BCC blob locations.
4. Calculating the BCC layout, headers, offsets, padding, and checksums.
5. Streaming snapshot file contents into the destination object or objects without materializing the complete shard locally.

Lucene segment contents normally do not need rewriting or reindexing. Repository chunks must be reassembled logically, and commit metadata
may need to be synthesized or adjusted. The direct converter must reproduce the relevant effects that the current path obtains by
restoring files, opening the engine, finalizing recovery, and committing.

The preferred initial representation is one logical BCC per shard. If object-size or multipart part-count limits make one physical object
invalid, the shard may use a bounded set of segment-only BCCs plus a final commit-bearing BCC that references them. The chain must be
bounded to avoid excessive startup reads, reference tracking, and garbage-collection work.

The existing `stateless.upload.max_size` setting is an upload trigger, not a hard BCC size limit. Current BCC upload code does not split an
oversized single commit and does not fully guard every provider part-count limit. The converter must validate the calculated object size,
part size, and part count before starting an upload.

### Optional server-side copy

For compatible source and destination stores, provider-native multipart copy may avoid routing most segment bytes through an Elasticsearch
node. For example, S3 can assemble a destination multipart object from generated header parts and `UploadPartCopy` ranges.

This is an optimization rather than a required mechanism. It may be unavailable because the source and destination use different
providers, accounts, credentials, encryption, compression, chunk boundaries, or minimum part sizes. A bounded streaming implementation is
required as the fallback.

## Multipart upload and resumption

Current BCC uploads already use provider-native multipart upload, including concurrent part uploads where supported. A failed upload is
retried as a whole; its upload ID and completed parts are not durably exposed for another node to resume.

Restore transfers need a resumable abstraction with operations equivalent to:

* Begin or discover an upload for an immutable object identity.
* List completed parts and their checksums.
* Upload or copy a specified part.
* Complete the upload conditionally.
* Abort an unusable upload.
* Read final object metadata for verification.

The object store should be authoritative for completed parts. If a provider cannot rediscover an upload, the task document may store the
upload ID as a locator, while the provider remains authoritative for which parts exist. If safe resumption is impossible, a worker starts a
new upload under a new attempt-specific identity.

Part boundaries should be deterministic from the source-file plan and BCC layout. A replacement worker can then validate existing parts
and resume without trusting byte-progress fields in the task document.

After completing an upload, the worker verifies object length and checksums and reads enough BCC metadata to prove that it describes the
expected restore, destination shard, and source plan. Only then may it publish the result to the task document as `SUCCESS`.

## Atomic and idempotent activation

The restore document and cluster state cannot be updated atomically. A coordinator may successfully activate shards and crash before it
marks the restore `DONE`. If a replacement blindly performs activation again after the shards have accepted writes, it could reset them to
the snapshot and lose those writes.

Normal stateless existing-store recovery provides some protection: it searches primary-term directories up to the current term, selects
the greatest `(primaryTerm, generation)` BCC, and replays later object-store translog operations. Thus an ordinary restart after activation
should recover BCCs and acknowledged writes newer than the staged restore. This must remain a secondary safety property, not the
idempotency protocol. Reinstalling a snapshot recovery source, republishing the restored BCC under a newer term, or explicitly pinning
recovery to the staged BCC could bypass latest-state discovery.

Finalization therefore writes an activation marker into `IndexMetadata` in the same cluster-state update that creates or activates routing.
There is no `STAGED` marker and no cluster-state update before copying.

The marker contains at least:

* Restore UUID or activation ID.
* Snapshot UUID and plan digest.
* Destination index UUID.
* A state meaning `ACTIVATION_COMMITTED`.

The final cluster-state task performs these checks and changes atomically:

1. Verify that the destination name is absent for a new-index restore, or that an existing closed destination still has exactly the
   identity and state allowed by the plan.
2. If a matching activation marker already exists, return success without modifying routing.
3. If another activation marker exists, fail with a conflict.
4. Create or update `IndexMetadata` using the preplanned destination UUID.
5. Add the activation marker.
6. Create or transition routing using normal stateless existing-store recovery from the already-published destination BCCs.
7. Apply the planned index blocks, aliases, and visibility changes.

The marker and routing transition are one cluster-state publication. Shards cannot become writable before the marker is visible. After a
coordinator crash, a replacement sees the matching marker, does not reset routing, waits for shard startup if necessary, and marks the
restore document `DONE`.

For a multi-index restore, activation may be one cluster-state task for all indices if atomic visibility is required, or one task per index
with one marker per index. In the latter case the restore document aggregates partial activation, and cancellation must not delete already
activated indices implicitly.

The simplest policy is to retain the activation marker as small restore provenance. If it is removed, the restore document must first be
durably marked `DONE`; marker cleanup is a later idempotent operation. Removing the marker before recording `DONE` recreates the crash
ambiguity.

## Relationship to existing restore and persistent tasks

The existing restore uses cluster state both for orchestration and shard status. Routing transitions and `RestoreInProgress` shard
completion are atomic in the same allocation cluster-state update, so it does not have the proposed design's external-state crash window.
The new activation marker restores that atomic boundary without placing transfer progress in cluster state.

Persistent tasks offer durable parameters and status, master-coordinated assignment, automatic reassignment, allocation-ID fencing,
cancellation, and wait APIs. Their lifecycle and status updates are stored in cluster state and allocated by the elected master. They are a
poor fit for a potentially large per-shard transfer queue with regular lease or progress changes, so this design uses indexed documents and
accepts duplicate work instead.

## Failure handling

Important failure cases and expected outcomes include:

* Coordinator fails while creating tasks: another coordinator recreates deterministic missing tasks and seals the plan.
* Worker fails during upload: its lease expires and another worker resumes or replaces the upload.
* Old worker continues after lease loss: it cannot publish its object reference; its immutable output becomes garbage.
* Upload completes before task publication: the replacement discovers and verifies the deterministic result or the attempt-specific object.
* All tasks finish but the coordinator fails before activation: another coordinator performs activation.
* Activation succeeds but the coordinator fails before updating the restore document: the activation marker makes the retry a no-op.
* Shard allocation or startup fails after activation: ordinary allocation retries; the coordinator does not reapply the snapshot.
* Source checksum or BCC verification fails deterministically: the shard task and restore fail with diagnostic information.
* Internal coordination index is temporarily unavailable: workers stop acquiring or renewing work; object-store outputs remain safe.
* Cancellation races with activation: the cluster-state task checks the current restore/activation intent immediately before applying the
  transition. An already committed activation is reported rather than silently reversed.

## Cleanup

Cleanup is asynchronous and reference-driven. It covers expired multipart uploads, lease-specific staging objects, outputs from stale
attempts, cancelled restores, and retained restore documents past their history period.

No BCC referenced by activated index metadata or by the authoritative result of a live task may be deleted. Cleanup operates on explicit
restore/attempt prefixes and immutable references rather than inferred byte-progress counters.

## Concurrency and resource controls

Workers must enforce limits for:

* Concurrent shard transfers per node and per restore.
* Aggregate source-read and destination-write bandwidth.
* Concurrent provider requests and multipart parts.
* Memory and temporary disk used for buffers.
* Fairness against normal stateless commit upload, snapshot creation, and search reads.

Restore upload work should have independent accounting and metrics. Useful measurements include pending and leased tasks, expired leases,
retry counts, logical and transferred bytes, throughput, active multipart parts, orphan cleanup, conversion time, verification time,
activation latency, and time until primaries start.

## Delivery sequence

1. Prototype direct conversion of one snapshot shard to a BCC and prove that existing stateless recovery can open it with an empty local
   cache.
2. Define the immutable BCC identity, commit metadata rules, provider size validation, and verification contract.
3. Add internal managed system-index descriptors and document mappings for restore and task records.
4. Implement deterministic planning, task sealing, leases, fencing, throttling, and worker recovery.
5. Implement resumable multipart upload for one provider and the portable streaming fallback.
6. Implement activation metadata and the atomic idempotent cluster-state task.
7. Add cancellation, retry controls, status APIs, history retention, and garbage collection.
8. Add remaining providers, optional server-side copy, scale tests, and guarded rollout.

## Required tests

At minimum, tests must cover:

* Failure at every boundary between restore creation, task creation, task sealing, copying, verification, publication, activation, and `DONE`.
* Lease expiry while the previous worker remains alive.
* Two workers completing identical and conflicting attempts.
* Multipart resumption and replacement after node loss.
* Provider object-size and part-count limits.
* Activation retry before shard startup, during startup, and after writes have been acknowledged.
* Recovery choosing BCC and translog state newer than the original restored BCC.
* A coordinator crash after activation but before its restore-document update.
* Destination name or UUID conflicts and an activation marker belonging to another restore.
* Cluster-manager changes during finalization.
* Internal-index unavailability and recovery.
* Cancellation before and after activation.
* Cleanup proving that referenced BCCs are retained and orphan attempts are removed.

## Open design questions

* Exactly which commit metadata must be synthesized to reproduce the current post-restore engine commit without materializing a full store.
* Whether the existing BCC format can always be emitted as a bounded-memory stream, including final checksums and padding.
* The physical partitioning rule for shards that exceed provider object or multipart limits.
* The provider-neutral API needed for discovering and resuming multipart uploads.
* Whether restore-over-existing-index is included initially or new-index restore ships first.
* Whether multi-index activation must be atomic or may complete index by index.
* Whether the activation marker is retained permanently as provenance or cleaned up after `DONE`.
* The exact completion condition: activation committed, primaries started, or all configured shard copies started.
* Which node roles are eligible to coordinate and perform transfer work.
