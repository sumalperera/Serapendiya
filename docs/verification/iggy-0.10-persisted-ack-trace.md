# Iggy 0.10 persisted ACK trace and process-kill probe

Date: 2026-11-15. Source pin: Apache Iggy `server-0.10.0`, commit
`91e0206367aad2d30259f7d45203832b08ae84e7`. This record combines source
inspection with one local synthetic process-kill probe. It is not power-loss,
corruption, syscall-tracing, performance, cross-platform or release evidence.

## Source trace: singleton, message durability persisted

The source exposes two related durability stages. The partition prepare journal
frontier protects prepared operations before they can help commit; materialized
message segments and indexes are then synchronized before the singleton
persisted client reply. These are distinct structures and publication points.

| Order | Source-backed action | What the source establishes |
| --- | --- | --- |
| 1 | The prepare is appended to the partition prepare journal. The persistence worker batches eligible prepares in `append_batch_buffered`. | [PartitionPersistence::append_batch](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/persistence.rs#L1490-L1530) writes a batch and invokes `journal.sync()` for durable work. |
| 2 | `PartitionPrepareJournal::sync` synchronizes its body barrier and journal file, then publishes the frontier. | [Journal sync](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/journal/src/partition_journal.rs#L698-L714) joins body and file barriers, calls `publish`, and advances `durable_head`. [Frontier publication](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/journal/src/partition_journal.rs#L1310-L1337) writes the selected slot and syncs the frontier file. |
| 3 | Persistence completion wakes the partition. It sends `PrepareOk` only after the durable head/checksum covers the prepare; `send_prepare_ok` also gates required superblock persistence. | [Persisted-ack path](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_partition.rs#L2355-L2393) and [PrepareOk gates](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_partition.rs#L13693-L13726). This is a prepare acknowledgment to the consensus protocol, not yet the client reply. |
| 4 | Once the singleton commit is selected, the persisted send takes the materialization path. The code writes committed message data and sparse index data to their segment files. | [Commit selection](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_partition.rs#L11414-L11472) sets materialization for persisted singleton sends. [Materialization I/O](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/io.rs#L560-L610) submits message and index writes; the [message writer](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/messages_writer.rs#L114-L140) and [index writer](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_index_writer.rs#L110-L120) expose their sync behavior. |
| 5 | Before commit publication, the persisted singleton path syncs the partition directory when segment names are dirty. The commit then publishes local commit state and sends the client reply. | [Commit directory barrier and publish transition](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_partition.rs#L10940-L10970), [directory I/O](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/io.rs#L690-L710), [reply delivery](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/iggy_partition.rs#L7258-L7275). |

The journal frontier in step 2 is published before `PrepareOk`; it is not a
second final frontier written after segment-directory sync. A later WAL
checkpoint can reclaim journal history after materialization. Do not flatten
these structures into one guessed syscall sequence. This source trace identifies
the main ordering, but it does not instrument syscalls, enumerate every
interruption cut, or independently establish filesystem durability semantics.

The documented durability contract says singleton `persisted` synchronizes
message/index data and newly created segment names before replying. This source
trace is consistent with that ordering at the selected path. It does not prove
missing-tail, corruption, or power-loss behavior; those require dedicated tests.

## Process-kill probe

Environment and setup: the same pinned debug server and matching CLI from the
[baseline smoke report](iggy-0.10-baseline-smoke.md), on macOS arm64, using the
existing temporary local data directory. Topic `feasibility/sample` had
message durability and consumer-offset durability explicitly set to
`persisted`, with one partition and one replica. No customer data was used.

Procedure and observed results:

1. Start the server against the same data directory with TCP on
   `127.0.0.1:28090`, HTTP disabled, one unpinned shard.
2. Run the matching CLI:
   `iggy message send feasibility sample sigkill-after-ack-20261115`.
   The CLI printed “Sent messages” and exited 0.
3. After the CLI returned, send SIGKILL to the single test server process.
   The server process exited with status 137; no graceful shutdown was requested.
4. Restart the server with the same data directory. Polling from offset 2
   returned `sigkill-after-ack-20261115` at offset 2; the CLI exited 0.
5. The persisted consumer-offset query returned current offset 2 and stored
   offset 1. This is consistent with the earlier explicit offset store and
   confirms the test message did not depend on advancing that stored offset.
6. Stop the restarted server with Ctrl-C; it reported clean shutdown.

This is one successful post-ACK **process-kill** observation on one local
configuration. The OS may have retained dirty cache contents after SIGKILL; this
says nothing about sudden power loss or kernel/device cache loss. It does not
test a crash between any two writes or sync calls, and it does not show how a
corrupted certified tail is handled. No latency or cross-platform conclusion
follows.

## Connector follow-up

At the pinned source, `poll_options` sets
`CommitPolicy::OnRead` directly in
[the sink runtime](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/connectors/runtime/src/sink.rs#L738-L746).
The runtime code inspected does not expose that choice through this poll-options
construction. A failing-plugin reproduction and an independently controlled
consumer/manual-offset comparison are still open; no connector failure test was
run here.

## Next evidence

- Instrument or inject each source cut around journal append, both journal/body
  syncs, frontier publication, segment/index writes and syncs, directory sync,
  commit publication and reply.
- Test damaged or missing acknowledged-tail data separately from process kill.
- Run filesystem/block replay only under its distinct authorization and report
  the filesystem, kernel, device model and replay cut points.
- Reproduce the sink offset behavior with a controlled failing plugin before
  choosing a fork change or an external consumer design.
