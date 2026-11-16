# Iggy 0.10 persisted ACK trace and process-kill probe

Date: 2026-11-16. Source pin: Apache Iggy `server-0.10.0`, commit
[`91e0206367aad2d30259f7d45203832b08ae84e7`](https://github.com/apache/iggy/tree/91e0206367aad2d30259f7d45203832b08ae84e7).
This record combines source inspection, one post-ACK server process-kill probe,
and a synthetic sink `OnRead` reproduction. It is not power-loss, storage
corruption, syscall tracing, performance, cross-platform or release evidence.

## Source trace: singleton, message durability persisted

The path has two linked durable structures: the partition prepare WAL and the
materialized message/index files. A volatile local commit value is not itself a
durability certificate.

| Order | Source-backed action | What the source establishes |
| --- | --- | --- |
| 1 | The persistence worker batches eligible prepares and appends them to the partition prepare journal. | [PartitionPersistence::append_batch](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/persistence.rs#L1490-L1530) invokes `journal.sync()` for durable work. |
| 2 | `PartitionPrepareJournal::sync` joins the body barrier and WAL-file sync, then publishes the frontier. | [Journal sync](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/journal/src/partition_journal.rs#L698-L714) waits for both barriers before [frontier publication](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/journal/src/partition_journal.rs#L1310-L1337) writes and syncs its selected slot and advances `durable_head`. |
| 3 | Persistence completion gates `PrepareOk` on durable head/checksum coverage; required superblock persistence is also gated. | [PrepareOk path](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_partition.rs#L13693-L13726). This is the prepare acknowledgement to consensus, not yet the client reply. |
| 4 | For a persisted singleton send, materialization writes and synchronizes message and sparse-index bytes before commit can advance. | Topic durability is carried in `TopicRuntimeOptions` from the parsed topic options ([option conversion](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/common/src/types/options/mod.rs#L1144-L1165)). [Partition storage setup](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/partition_storage.rs#L557-L559) derives `persisted` from `runtime.durability.is_persisted()` and passes that same flag to both the [message writer](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/partition_storage.rs#L591-L600) and [index writer](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/partition_storage.rs#L616-L622). [MessagesWriter::save_frozen_batches_at](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/messages_writer.rs#L114-L140) awaits its conditional `fsync`; [MessagesWriter::fsync](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/messages_writer.rs#L160-L175) awaits `File::sync_data`. [IggyIndexWriter::save_indexes_at](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_index_writer.rs#L110-L120) likewise awaits its conditional [sync-data call](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_index_writer.rs#L164-L178). [MaterializationIoJob](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/io.rs#L600-L618) joins and awaits both write futures, and reports failure if either fails. The commit handler awaits the I/O job and returns through the failure path on error; it publishes only after materialization succeeds ([commit handler](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_partition.rs#L10846-L10893), [materialization result acceptance](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_partition.rs#L10704-L10724)). |
| 5 | The persisted singleton path synchronizes the partition directory when segment names are dirty, then publishes commit state and replies. | [Commit directory barrier and phase](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_partition.rs#L10935-L10975), [directory sync I/O](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/io.rs#L690-L710), and [reply delivery](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_partition.rs#L7258-L7275). |

**Which structure certifies an acknowledged message?** Before the WAL checkpoint
reclaims it, the published prepare-WAL frontier certifies the durable prepare
(and any referenced message bodies); the persisted singleton commit is not
replied to until message/index file syncs and any required segment-directory
sync also complete. On recovery, the singleton path restores commit through
the recovered WAL head and replays prepares
([recovery code](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_partition.rs#L1790-L1825)).
The handoff is explicit in the source. The persistence worker awaits
[`checkpoint_files`](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/persistence.rs#L1432-L1454);
that method syncs listed materialized files and directories before publishing
the WAL checkpoint ([checkpoint implementation](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/journal/src/partition_journal.rs#L584-L628)).
Recovery starts from the checkpoint op and recovered WAL head, replays prepares,
and for a singleton restores commit through the recovered head
([recovery path](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_partition.rs#L1807-L1835)).
So the source-level answer is a handoff: before checkpoint reclamation, the
published WAL frontier is the recoverable prepare authority; after the synced
checkpoint, segment/index state is the recovered message representation. The
local commit value alone is not the certificate. This establishes source order,
not filesystem semantics or every crash cut.

## Server process-kill probe

Environment and setup: the matching debug server and CLI on macOS arm64 used a
temporary data directory. Topic `feasibility/sample` had one partition and
one replica, with message and consumer-offset durability explicitly
`persisted`.

After sending `sigkill-after-ack-20261115`, the CLI exited 0. The server was
then sent SIGKILL and exited 137. On restart, polling from offset 2 returned
that payload at offset 2; the stored consumer offset remained 1. The server
then stopped cleanly. This is one post-ACK process-kill observation: the OS
page cache survived, so this says nothing about sudden power loss, device-cache
loss, corruption or a kill between write/sync calls.

## Sink `OnRead` reproduction

The full procedure and request/effect observations are in the
[synthetic sink reproduction](iggy-connector-onread-reproduction.md).
In summary, the built-in pinned sink sets `CommitPolicy::OnRead` directly in
[`poll_options`](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/connectors/runtime/src/sink.rs#L738-L746);
the inspected sink configuration has no commit-policy field.

On the local persisted single-replica setup, a synthetic HTTP endpoint returned
500 for message offset 1. The group offset moved from 0 to 1, the runtime
reported the callback error, and no effect was recorded. Restarting the sink
against a success endpoint did not send that message again. In the corrected
crash cut, message offset 3 was held before any endpoint effect; the group
offset was already 3. SIGKILL terminated the runtime with status 137; after
restart the message was not requested again and the endpoint still showed zero
effects at observation time. This demonstrates the sink's poll-before-callback
loss window in these runs, not an exactly-once or general delivery guarantee.

The SDK defines `Manual` and `Auto` commit policies in
[`poll_options.rs`](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/common/src/types/message/poll_options.rs#L28-L76).
A separate explicit-offset CLI poll without `--auto-commit` returned offset 3
without creating a stored offset for regular consumer 0; the sink consumer
group remained at 3. This is a narrow manual-read observation, not a tested
replacement sink or application checkpoint protocol. The [follow-up reproduction](iggy-connector-onread-reproduction.md) exercised
both policies with a standalone partition consumer. Each redelivered after a
simulated effect followed by callback failure; the successful retry stored
offset 0. Group rebalancing and downstream lost-response behavior remain open.

## Commit and recovery cut-point inventory

This is a source-derived test plan, not a crash-safety result. “Expected after
restart” records the behavior implied by the source ordering; every row still
needs deterministic interruption evidence before it can be treated as proven.

| Cut | Expected after restart from the source ordering | Evidence status / needed probe |
| --- | --- | --- |
| Prepare body/WAL writes or either sync, before frontier publication starts | No prepare ACK can have been sent. With both prior frontier slots still valid, recovery uses the exact published prefix and discards any unpublished file tail; this is an inference to verify at each write/sync cut. | Source puts both body/WAL barriers before frontier publication ([sync path](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/journal/src/partition_journal.rs#L698-L714)); inject at each write/sync boundary and check truncation/refusal behavior. |
| Frontier slot write or sync, before PrepareOk | No prepare ACK has been sent. If the newest slot verifies, recovery uses it. If the slot is unreadable, recovery scans the unpublished record tail: it adopts only a complete valid chain and refuses on damage because it cannot treat possibly acknowledged records as disposable. Any operation recovered here has an unknown client outcome. | This behavior is specified in [frontier publication](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/journal/src/partition_journal.rs#L1299-L1333) and [unpublished-tail recovery](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/journal/src/partition_journal.rs#L911-L987); crash-inject around slot write/sync and corrupt/torn tail cases. |
| After durable frontier / PrepareOk, before materialization completes | The prepare is recoverable from the WAL. Restart should reconstruct the committed singleton operation from the recovered head and replay/repair message materialization; client reply has not yet been sent. | Recovery source restores the singleton commit to head and replays prepares ([recovery](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_partition.rs#L1807-L1835)); requires restart probes at this cut. |
| During message/index write or while only one writer's sync has completed | The WAL remains the recovery source until checkpoint reclamation. A partial or mismatched materialization must not be served as certified data; restart must recover or refuse safely. | Both writer futures are joined and awaited ([materialization](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/io.rs#L600-L618)); inject writes and each sync separately and inspect recovery. |
| Both file syncs complete, before required segment-directory sync | Client reply is still gated. WAL coverage remains; restart must not assume a newly created segment name is durable just because file contents synced. | Commit phase waits for dirty segment-directory sync before publication ([phase driver](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_partition.rs#L10935-L10964)); inject before/after directory sync. |
| Directory sync completes, before commit publication/reply | The data barriers completed, but the caller has no ACK yet. On restart the operation may be recovered from the durable WAL; the client outcome remains unknown. | Handler reaches publication only after I/O phases complete ([commit handler](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_partition.rs#L10869-L10893)); process-kill at this exact cut is untested. |
| Commit publication completes, before client reply arrives | The operation should remain recoverable; the client may retry because it did not observe the reply. Duplicate handling is a separate protocol question. | Source orders publish_pending_commit before reply delivery ([reply path](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_partition.rs#L7258-L7263)); inject between publication and delivery. |
| Materialized file/directory barriers complete, before WAL checkpoint publication | Recovery should still have WAL history if the checkpoint did not publish; if it did publish, the already-synced materialized representation is the recovery basis. | The checkpoint worker awaits file and directory barriers before checkpointing ([worker](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/persistence.rs#L1432-L1454), [journal checkpoint](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/journal/src/partition_journal.rs#L584-L628)); inject during checkpoint publication and verify both observed states. |

The existing SIGKILL probe covers only the quiet post-ACK case. It does not
exercise any row above, prove the restart expectations, or establish power-loss
semantics.

## Remaining evidence

- Instrument or inject each cut around journal append, body/WAL syncs, frontier
  publication, message/index writes and syncs, directory sync, commit
  publication and reply.
- Test missing or damaged acknowledged-tail data separately from process kill.
- Run filesystem/block replay only under its distinct authorization and report
  filesystem, kernel, device model and fixed replay cut points.
- Test destination-success/lost-response ambiguity, group rebalancing, retries
  and partial batches with the custom SDK consumer.
