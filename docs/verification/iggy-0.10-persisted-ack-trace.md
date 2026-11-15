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
| 4 | For a persisted singleton send, materialization writes message and sparse-index bytes. | [Commit selection](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_partition.rs#L11414-L11472) selects materialization; [MaterializationIoJob](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/io.rs#L560-L610) runs the writes. The file syncs were not omitted: [MessagesWriter::save_frozen_batches_at](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/messages_writer.rs#L114-L140) calls `fsync` when its persisted flag is set and the save completes; [MessagesWriter::fsync](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/messages_writer.rs#L160-L175) calls `File::sync_data`. [IggyIndexWriter::save_indexes_at](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_index_writer.rs#L110-L120) similarly calls [its `sync_data` fsync](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_index_writer.rs#L164-L178) when persisted and bytes were written. The persisted option is passed into both writers by [partition storage setup](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/partition_storage.rs#L590-L630). |
| 5 | The persisted singleton path synchronizes the partition directory when segment names are dirty, then publishes commit state and replies. | [Commit directory barrier and phase](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_partition.rs#L10935-L10975), [directory sync I/O](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/io.rs#L690-L710), and [reply delivery](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_partition.rs#L7258-L7275). |

**Which structure certifies an acknowledged message?** Before the WAL checkpoint
reclaims it, the published prepare-WAL frontier certifies the durable prepare
(and any referenced message bodies); the persisted singleton commit is not
replied to until message/index file syncs and any required segment-directory
sync also complete. On recovery, the singleton path restores commit through
the recovered WAL head and replays prepares
([recovery code](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_partition.rs#L1790-L1825)).
The WAL can retire that coverage only after
[`checkpoint_files`](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/journal/src/partition_journal.rs#L584-L625)
has synchronized materialized files and directories before checkpointing.
Thus the answer is a handoff: the synced WAL frontier covers the operation
until synced materialization is checkpointed; afterward the materialized
segment/index state is the recovered representation. This is the source-level
ordering, not a claim that this inspection independently proves filesystem
semantics or every crash cut.

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
replacement sink or application checkpoint protocol. A custom SDK consumer
using manual store-after-effect or a processing-based auto policy remains to
be exercised.

## Remaining evidence

- Instrument or inject each cut around journal append, body/WAL syncs, frontier
  publication, message/index writes and syncs, directory sync, commit
  publication and reply.
- Test missing or damaged acknowledged-tail data separately from process kill.
- Run filesystem/block replay only under its distinct authorization and report
  filesystem, kernel, device model and fixed replay cut points.
- Test custom SDK `Manual`/`Auto` processing policy, destination-success /
  lost-response ambiguity, retries and partial batches.
