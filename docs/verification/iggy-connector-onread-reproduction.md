# Iggy 0.10 sink OnRead reproduction

Date: 2026-11-16. Source pin: Apache Iggy `server-0.10.0`, commit
[`91e0206367aad2d30259f7d45203832b08ae84e7`](https://github.com/apache/iggy/tree/91e0206367aad2d30259f7d45203832b08ae84e7).
Environment: macOS arm64, Rust/Cargo 1.99.1, debug binaries, one local
single-replica broker with a temporary data directory. The broker was configured
with message and consumer-offset durability set to `persisted`. All payloads
and HTTP requests were synthetic. This is process-level connector evidence on
one local setup, not power-loss, filesystem, release-build, performance or
cross-platform evidence.

## Question and source configuration

The sink runtime's `poll_options` sets `commit: CommitPolicy::OnRead`
directly in [sink.rs](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/connectors/runtime/src/sink.rs#L738-L746).
The setting is not read from `StreamConsumerConfig`; its [pinned definition](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/connectors/runtime/src/configs/connectors.rs#L270-L286)
contains batch and wait settings but no commit-policy field. In the inspected
pinned runtime, sink offset policy is therefore hardcoded to `OnRead`.

The SDK has other policies: `Manual` and `Auto` with a processing-related
commit point are defined in
[poll_options.rs](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/common/src/types/message/poll_options.rs#L28-L76).
This reproduction did not change the runtime or test those policies through a
custom sink. They remain candidate approaches for a separately controlled
consumer, not evidence that the built-in sink can select them.

## Synthetic setup

A local HTTP server stood in for the downstream. It recorded request bodies,
could hold a request before recording a destination effect, or return HTTP 500.
The Iggy stream `onread_probe`, topic `sample`, partition 0 used consumer
group `onread_probe_group`; the HTTP sink used batch length 1 and zero retries.
The same connector runtime, plugin, broker data directory and group were reused
across restarts.

The observed sequence was:

| Case | Message offset | Destination behavior | Offset/result |
| --- | ---: | --- | --- |
| Pause | 0 | Endpoint received the message and held the request before recording an effect. While held, the endpoint showed one request and zero effects. The initial stored offset read was 0, which is also the initial boundary and therefore does not by itself prove advancement. After release, the runtime completed the callback and the endpoint recorded the effect. | Confirms the destination callback can still be in flight after poll. The offset observation at zero is inconclusive on advancement. |
| HTTP error | 1 | Endpoint returned HTTP 500; runtime logged the request failure and one consumed message. No destination effect was recorded. | Before this poll the group stored offset was 0; afterward current/stored offset was 1. On restart with the endpoint changed to success, the failed message was not requested again. The failure therefore leaves a consumed message without a destination effect in this run. |
| Runtime SIGKILL | 3 | A separate endpoint received the message and blocked before recording any effect. While blocked, current/stored group offset was 3 (previous stored offset was 2). The runtime was then sent SIGKILL and exited 137. | After restart against the same broker and group, the blocked message was not requested again. The endpoint still showed one request and zero effects at the observation. This is a process-kill cut after poll and before the synthetic destination effect. |

The intervening offset 2 message was delivered successfully to the synthetic
endpoint during setup; it is not part of the error or crash result. An initial
misrouted hold request reached the nonblocking endpoint and completed; it was
excluded from the crash evidence. The reported crash row is the corrected run
against the separate blocking endpoint.

## Manual-offset comparison

After the runtime crash, the matching CLI read offset 3 explicitly with
`message poll --offset 3` and without `--auto-commit`. It returned the
synthetic message. A query for regular consumer 0 then reported no stored offset;
the sink group's stored offset remained 3. This demonstrates the CLI's explicit
read does not automatically store an offset when `--auto-commit` is omitted.
It is a narrow CLI observation, not a tested replacement sink or a complete
application-level checkpoint protocol.

## What this establishes

- In this pinned configuration, the built-in sink polls with `OnRead`.
- The sink can record a polled offset before its HTTP callback returns: the
  failure and blocked-request cases show the stored group offset at the
  polled-message boundary while no successful downstream effect existed.
- On restart in these two cases, the sink did not request the polled message
  again. A failure after poll can therefore produce message loss at the
  downstream boundary unless the destination or an outer application protocol
  supplies reconciliation.
- The SDK and CLI expose manual/other commit concepts, but this does not show
  that the built-in sink can configure them or establish their crash behavior.

## SDK-controlled offset follow-up

A temporary Rust example using the pinned public SDK `IggyConsumer` API was
run against the same local broker, now as standalone consumers assigned to
partition 0 (not consumer groups). Two fresh consumer IDs and empty effect
ledgers were used.

| Policy | First attempt | Restart | Observed result |
| --- | --- | --- | --- |
| `Manual` | The callback appended the message payload to a local effect ledger and synced it, then simulated failure before calling `commit(checkpoint)`. | The same consumer ID received the same message at offset 0, appended the effect again, then called `commit` successfully. | Ledger contains the same message twice; stored offset is 0. Manual storage preserves retry when the caller withholds the commit, with at-least-once duplicate risk after an effect/commit gap. |
| `Auto` with `CommitAfter::Processing`, `every: Message` | `consume_messages` callback appended and synced the effect, then returned an error. The SDK returned a handler error. | The same consumer ID received offset 0 again; the callback succeeded, then consumer shutdown flushed the counted position. | Ledger contains the same message twice; stored offset is 0. Processing-based auto commit left the failed callback uncounted and stored after successful handling. |

The CLI's regular-consumer offset query showed `Stored offset = 0` after
each successful retry. The two effect ledgers each contain two identical
payload lines, one from the effect-before-error attempt and one from the
successful retry. The example and ledgers were temporary artifacts; no SDK
source or production code was changed.

This validates SDK-controlled behavior for a standalone partition consumer in
one local debug run. It does not validate group assignment/rebalance behavior,
broker failover, offset-storage failure, or a downstream system's response
ambiguity. The example's file append models a durable effect only for ordering;
it is not a transactional destination. The effect and offset remain separate
systems, so an effect that succeeds while its response is lost can still be
duplicated after restart. Use destination idempotency or reconciliation where
available; do not claim exactly-once side effects.

## Limits and next test

This is a small synthetic reproduction, not an exhaustive connector state
machine. It does not test destination acceptance followed by a lost HTTP
response, retry ambiguity, partial batches, multiple partitions, consumer-group
rebalance, failover, offset durability loss, or an actual process crash inside
the plugin after a side effect. The held-request case kills the runtime before
the endpoint records an effect; it does not model a remote service that commits
the effect and then loses its response.

Next, test destination-success/lost-response ambiguity and group rebalance or
failover with the custom consumer; the current local run already shows the
expected at-least-once duplicates in both SDK policies. Do not infer
exactly-once delivery from these results.
