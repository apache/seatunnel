# PostgreSQL CDC bounded-read audit

<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

## Decision and scope

**Needs design agreement before implementation. This document does not enable bounded WAL reads.**
It is the PostgreSQL slice of [#11739](https://github.com/apache/seatunnel/issues/11739), not a
claim that the other connectors have been audited. PostgreSQL `stop.mode` remains `never` by
default and as its only supported value. `startup.mode = snapshot-only` is a separate existing
capability, not an implementation of an incremental stop boundary.

Source baseline: `b37af3a9bf634735cabe4908014c2d390b288558`; Debezium `1.9.8.Final`.
The issue body and both comments were read on September 16, 2026. The follow-up asks for offset
ordering, enforcement points, and actual job completion evidence before connector-specific PRs.
There was no PostgreSQL stop-mode ownership claim in that conversation. Targeted open-issue/PR
searches for `11739` and `"stop.mode" postgres` found no separate PostgreSQL stop-mode implementation;
that is a dated search result, not a reservation or proof against unpublished work.

## Overlapping work

[PR #11556](https://github.com/apache/seatunnel/pull/11556), by davidzollo, is open at
`ba7c8bd743c8a97cd308bf322e3cbde58b58c552` in this audit. It owns bounded snapshot WAL backfill,
including `PostgresWalFetchTask.PostgresWalSplitReadTask`, snapshot-reader slot isolation,
enumerator slot preparation, and watermark acquisition. Its wrapper sets
`PostgresOffsetContext.streamingStoppingLsn`, calls the existing Debezium streaming source,
checks producer failure, and dispatches END through the existing dispatcher.

Reuse that architecture after review and integration; do not copy its changes into an independent
reader. Snapshot reconciliation and public incremental termination need distinct policies even
if they eventually share the bounded reader. At this head the outer incremental `execute()` still
constructs the unbounded source. Therefore #11556 does not itself implement `stop.mode`.

The September 12 review still requests a dev sync, per-split table-filter escaping, and validation
of the effective slot name after Debezium property overrides, with tests. Earlier discussion also
identifies a cleanup documentation mismatch and crash-orphaned backfill slots. These are open-PR
findings, not new fixes or independently reproduced database incidents in this audit.

[PR #11029](https://github.com/apache/seatunnel/pull/11029) also changes PostgreSQL/OpenGauss source
ownership and option isolation. Coordinate its eventual entry points; a PostgreSQL-only option
cannot currently be enabled safely by editing the shared option object alone.

## Feasibility by contract

### Configuration: needs a PostgreSQL contract

`PostgresSourceOptions.STOP_MODE` declares only `NEVER`. Both connector factories use that option.
`StopConfig.getStopOffset()` routes `SPECIFIC` to `OffsetFactory.specific(String, Long)`;
`LsnOffsetFactory` rejects that overload. Its map overload is not used by `StopConfig`.
Merely allowing `SPECIFIC` fails later instead of providing a bounded reader.

Agree on an LSN representation and validation before adding an option: PostgreSQL `X/Y` text,
unsigned ordering, invalid/reserved sentinel rejection, and stop-before-start handling. Do not
silently reinterpret the MySQL filename/position contract or use a signed long comparison.
Defer `latest` and timestamp stop modes. `latest()` samples a WAL location, not a proven
decoded transaction boundary; its capture phase and persistence need their own specification.

### Offset ordering: event order is available, transaction completion is not implied

`LsnOffset.compareTo()` compares `lsn`, with explicit never-stop handling. `getLsnCommit()` reads
`lsn_commit` with an event-LSN fallback. Equal event LSNs can have different committed LSNs.
For example, an offset with `lsn=200, lsn_commit=100` compares after a stop offset of 150 even
though its committed position has not reached 150. The accompanying tests characterize this
distinction; they do not show an observed production transaction trace.

In Debezium's `PostgresStreamingChangeEventSource.processMessages()`, the stopping predicate
uses `lastCompletelyProcessedLsn`. This advances on `message.isLastEventForLsn()` before the
transactional/DML branches. In `PgOutputReplicationMessage`, that method returns true for
ordinary DML messages too. Completing an event LSN is not proof that COMMIT has been processed.
The [logical replication protocol](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html)
also distinguishes the commit LSN from the transaction end LSN.

**Proposed policy, not accepted semantics:** include complete transactions whose commit boundary
is at or before the configured bound; exclude complete transactions beyond it. A transaction
straddling the bound must not be partially emitted. Agreement must specify whether the bound is
commit-record LSN or transaction end LSN and how each supported decoder exposes it before rows
are released. If this needs transaction buffering or new decoder hooks, design that separately;
do not add unbounded buffering or vendor Debezium's private loop in this slice. Rounding upward
to include a crossing transaction is a different, over-read policy and needs explicit approval.

### Idle WAL: blocked on a reliable progress observation

Debezium's empty-poll path emits a heartbeat from existing offset state; it does not advance
`lastCompletelyProcessedLsn`. WAL can reach a location without producing a selected-table event
at that location. A future message beyond the target can also be dispatched before the next
loop-condition check. Neither case establishes a strict public stop contract.

#11556 removes `currentTransactionId()` from `PostgresUtils.currentLsn()` to avoid generating
additional WAL while sampling a watermark. That removes one cause of empty boundary gaps;
it is not proof that every arbitrary user LSN is decodable or that an idle job terminates.
Its offset-context stopping flag also selects Debezium's pre-snapshot catch-up transaction
behavior, so setting it for a normal incremental job requires a separate lifecycle check.

Require proof of decoder progress through the boundary even when selected tables are quiet.
Do not use elapsed time, an empty queue, the server's WAL end alone, or another consumer's slot
acknowledgement as proof. A marker/write-based solution would introduce privileges, plugin
compatibility and source-side effects; it requires explicit design agreement, not a hidden write.

### END, queue drain and FINISHED: completion signal and unproven drain guarantee

For an assigned finite split, `IncrementalSourceStreamFetcher.isBoundedReadFinished()` requires
`taskStarted`, `executing == false`, `streamFetchTask.isRunning() == false`, and a non-null,
non-never stop offset. `pollSplitRecords()` returns null when that predicate is true and its
record iterator has no next element. `IncrementalSourceReader.onSplitFinished()` handles incremental
completion. END is not a completion condition in the stream fetcher: MySQL's bounded producer
dispatches END, while `IncrementalSourceScanFetcher` consumes it to end snapshot backfill.
Do not turn that producer convention into a stream-reader requirement.

The polling gate and the later completion check are separate observations, not an atomic final
drain. Source inspection suggests a possible interleaving: `queue.poll()` returns empty while the
producer is running, then the producer enqueues its final batch and finishes before the later
completion check. The fetcher could then return null with records still queued. This is
**source-level inference, not a reproduced failure**; scenario 5 must validate this window rather
than assume the queue-drain guarantee is already established.

The producer must prove successful boundary completion and expose `isRunning() == false`.
Current `PostgresWalFetchTask` sets `taskRunning` true at entry and clears it only in `shutdown()`.
Simply substituting #11556's wrapper would leave that flag true. Producer failure and cancellation
must not be reported as successful bounded completion. `context.isRunning()` after a delegate
returns is not by itself evidence of reaching the bound: Debezium can return early when streaming
is disabled. Preserve error propagation and prove final eligible-row delivery, split exhaustion,
and Zeta job FINISHED in order. Do not rewrite the shared fetcher to compensate for a PostgreSQL
producer that cannot prove its end.

### Checkpoint and replication slot: needs terminal lifecycle proof

`IncrementalSplit` carries startup and stop offsets; restoration must retain the original bound.
`PostgresSourceFetchTaskContext.loadStartingOffsetState()` restores the event, commit and completely
processed positions. `PostgresWalFetchTask.commitCurrentOffset()` forwards commit LSNs to Debezium.
Debezium closes its replication connection when execution returns and ignores later offset commits
if the stream no longer exists. Test this ordering against the final checkpoint; a stopped reader
does not prove that the slot acknowledgement or sink commit is durable.

Preserve the default retained main slot. Distinguish success, graceful cancellation, restart and
process death before claiming drop-on-stop behavior. Dropping the slot before recovery is durable
can invalidate replay. #11556's backfill slot cleanup is a separate ownership problem and must
remain in that workstream. Do not conflate a Java `finally` cleanup with server-side temporary-slot
guarantees, and do not clean up slots belonging to another job.

### OpenGauss: shared path, not independently qualified

`OpengaussIncrementalSourceFactory` constructs `PostgresIncrementalSource` and reuses its STOP_MODE;
the OpenGauss module supplies its own PostgreSQL connection/replication classes. Therefore changing
the shared option enables a new mode there too. Keep OpenGauss at `never` unless decoder, idle
progress, slot lifecycle and restart behavior are separately demonstrated. Coordinate isolation
with #11029; do not treat PostgreSQL E2E results as OpenGauss evidence.

## Acceptance scenarios for a later runtime change

The following are required scenarios, **not passing tests supplied by this audit**. Extend
`PostgresCDCIT` and existing CDC base tests rather than creating an independent container suite.
Reuse the MySQL bounded-read E2E completion pattern, not its binlog ordering assumptions.

1. **Configuration:** default and explicit `never` retain their behavior. Reject malformed LSNs,
   never-stop sentinels, unsupported plugin/mode combinations and stop-before-start with useful
   errors. Assert PostgreSQL-only enablement does not broaden OpenGauss options.
2. **Transaction boundary:** with a slot established before writes, produce multi-row transactions
   T1, T2 and T3. Capture the selected decoder's actual commit and end positions, not just a later
   `pg_current_wal_lsn()` sample. For a bound at T2's agreed boundary, require all T1/T2 rows and no
   T3 rows. For a bound inside T2, require all-or-none according to the approved policy, never a
   prefix. Exercise multiple messages sharing an LSN and numeric sign-boundary ordering.
3. **Idle boundary:** cover no selected rows, only excluded-table writes, no decodable record at
   the requested position, and a future bound not reached yet. An already-reached idle bound must
   finish without the test injecting a rescue write; a future bound must not prematurely finish.
   Test `pgoutput` and `decoderbufs` separately where advertised, with transaction metadata on/off.
4. **Snapshot interaction:** include concurrent DML during an exactly-once initial snapshot and
   bounds before/inside/after its watermark range. Reject incompatible combinations rather than
   silently returning snapshot rows newer than a strict historical cutoff. Require #11556's
   reconciliation and filter tests, including legal table names with regex metacharacters.
5. **Queue drain:** hold a sink behind a deterministic gate, produce more than one queue batch,
   then reach the bound. Release the gate and require every eligible row, no post-bound rows,
   split exhaustion and actual FINISHED, without requiring END as the stream completion signal.
   Separately gate the final enqueue after an empty queue poll and producer completion before the
   later completion check; require the final batch to be delivered before split exhaustion. This
   targets the source-inferred, unreproduced window above. Empty output after filtering is not a
   failure; producer exceptions, cancellation and disabled streaming must not count as successful
   completion.
6. **Restart:** checkpoint mid-transaction and after boundary consumption but before final drain;
   restore with the same bound, then also fail over after the terminal checkpoint. Require the
   approved replay guarantee, final exact sink state, no reopened unbounded stream and completion
   without new source writes. Include old offset maps without `lsn_commit` and snapshot watermarks
   with/without `txId`; deserialization alone is not restart proof.
7. **Slots:** observe slot existence, active owner and acknowledged LSN before stop and after
   final checkpoint/close. Test retained default, explicit drop policy, normal completion,
   graceful cancellation and abrupt worker loss. Verify restart remains possible where promised;
   do not remove unrelated slots. Backfill cleanup is qualified separately under #11556.
8. **End-to-end status:** poll Zeta's job status to FINISHED with a bounded timeout, join all job
   futures and assert exit status and exact sink rows. Do not cancel the job to make the assertion
   pass. Keep failure cleanup in `finally`, use dynamic ports and condition-based gates, and
   collect last status, offsets and slot state on timeout. PostgreSQL success does not qualify
   OpenGauss, other engines or an entire CI workflow.

## Reproduction and evidence limits

The characterization tests added to `PostgresSourceConfigFactoryTest` run `ConfigValidator` with
the actual PostgreSQL STOP_MODE option: omitted/`never` succeeds, while `specific`, `latest` and
`timestamp` fail validation. This is an isolated option-contract reproduction, not job submission.
`LsnOffsetTest` adds the event/commit distinction, equal-event-LSN case and unsigned-order case.
These tests should pass against the unchanged production baseline. A later implementation must
replace rejection assertions only for the modes it actually qualifies.

Run the focused checks from the repository root with the supported Java toolchain:

```shell
./mvnw -pl seatunnel-connectors-v2/connector-cdc/connector-cdc-postgres \
  -Dtest=PostgresSourceConfigFactoryTest,LsnOffsetTest test
```

This slice adds no production code, stop option, E2E success claim or release/incident evidence.
It does not import #11556, resolve that PR's review findings or close the umbrella issue.
Implementation can proceed after boundary and idle-progress agreement, integration or agreed
extraction of the overlapping reader, OpenGauss isolation, and the above acceptance coverage.

## Source map

- PostgreSQL module: `source/PostgresSourceOptions`, `source/offset/LsnOffset`,
  `source/offset/LsnOffsetFactory`, `source/reader/wal/PostgresWalFetchTask`,
  `source/reader/snapshot/PostgresSnapshotFetchTask`, `source/reader/PostgresSourceFetchTaskContext`.
- CDC base module: `config/StopConfig`, `source/enumerator/IncrementalSplitAssigner`,
  `source/split/IncrementalSplit`, `source/reader/external/IncrementalSourceStreamFetcher`,
  `source/reader/IncrementalSourceReader`.
- Debezium [streaming source at v1.9.8.Final](https://github.com/debezium/debezium/blob/v1.9.8.Final/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/PostgresStreamingChangeEventSource.java)
  and [pgoutput message](https://github.com/debezium/debezium/blob/v1.9.8.Final/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/connection/pgoutput/PgOutputReplicationMessage.java).
