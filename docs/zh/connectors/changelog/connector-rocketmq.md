<details><summary> Change Log </summary>

| Change | Commit | Version |
| --- | --- | --- |
|[Fix][Connector-V2][RocketMQ] Close consumer client before interrupting its executor (#12099)|https://github.com/apache/seatunnel/commit/934276b2f7|3.1.0|
|[Fix][Connector-V2] Keep RocketMQ group offsets when a later topic has no retry route (#12648)|https://github.com/apache/seatunnel/commit/07f32fffbd|3.1.0|
|[Improve][Connector-V2] Correct two RocketMQ comments that no longer hold (#12438)|https://github.com/apache/seatunnel/commit/524312d741|3.1.0|
|[Improve][Connector-V2] Note the multi-topic early return in currentOffsets (#12417)|https://github.com/apache/seatunnel/commit/6934fd0062|3.1.0|
|[Fix][Connector-V2] Surface RocketMQ group-offset lookup failures instead of rewinding (#12349)|https://github.com/apache/seatunnel/commit/b70652be8a|3.1.0|
|[Fix][Zeta] Reuse fixed slots after master failover (#11458)|https://github.com/apache/seatunnel/commit/a9cda80f4b|3.0.0|
|[Improve][Common] Add HashUtils.bucketIndex for hash-to-bucket routing (#11987)|https://github.com/apache/seatunnel/commit/be53a1d3d1|3.0.0|
|[Improve][Connector-V2] Migrate RocketMQ Source validation to declarative OptionRule (#11158)|https://github.com/apache/seatunnel/commit/c8fb49358a|3.0.0|
|Test/rocketmq restore e2e (#10778)|https://github.com/apache/seatunnel/commit/6d2a104659|3.0.0|
|[Improve][Connector-V2] Complete OptionRule declarations for RocketMQ source and sink (#10701)|https://github.com/apache/seatunnel/commit/7becf69b70|3.0.0|
|[Feature][Connector-V2] Support multi-table read for RocketMQ source (#10619)|https://github.com/apache/seatunnel/commit/85615468b7|3.0.0|
|[Fix][Core] Add shade module for apache commons lang3 (#9895)|https://github.com/apache/seatunnel/commit/abb9124b05|2.3.13|
|[Improve][API] Optimize the enumerator API semantics and reduce lock calls at the connector level (#9671)|https://github.com/apache/seatunnel/commit/9212a77140|2.3.12|
|[improve] rocketmq options (#9251)|https://github.com/apache/seatunnel/commit/4cbe3b9172|2.3.12|
|[Feature][Checkpoint] Add check script for source/sink state class serialVersionUID missing (#9118)|https://github.com/apache/seatunnel/commit/4f5adeb1c7|2.3.11|
|[Improve][Connector-V2] RocketMQ Source add message tag config (#8825)|https://github.com/apache/seatunnel/commit/5913e8c35f|2.3.10|
|[Improve][Connector-V2] Add optional flag for rocketmq connector to skip parse errors instead of failing (#8737)|https://github.com/apache/seatunnel/commit/701f17b5d4|2.3.10|
|[Improve][Connector-V2] RocketMQ Sink add message tag config (#7996)|https://github.com/apache/seatunnel/commit/97a1b00e48|2.3.9|
|[Feature][Restapi] Allow metrics information to be associated to logical plan nodes (#7786)|https://github.com/apache/seatunnel/commit/6b7c53d03c|2.3.9|
|[Fix][Connector-V2] Fix some throwable error not be caught (#7657)|https://github.com/apache/seatunnel/commit/e19d73282e|2.3.8|
|[Feature][Kafka] Support multi-table source read  (#5992)|https://github.com/apache/seatunnel/commit/60104602d1|2.3.6|
|[Fix][connector-rocketmq] commit a correct offset to broker &amp; reduce ThreadInterruptedException log (#6668)|https://github.com/apache/seatunnel/commit/b7480e1a89|2.3.6|
|[fix][connector-rocketmq]Fix a NPE problem when checkpoint.interval is set too small(#6624) (#6625)|https://github.com/apache/seatunnel/commit/6e0c81d492|2.3.5|
|[Test][E2E] Add thread leak check for connector (#5773)|https://github.com/apache/seatunnel/commit/1f2f3fc5f0|2.3.4|
|[Fix] [Connector] Rocketmq source startOffset greater than endOffset error (#6287)|https://github.com/apache/seatunnel/commit/cd44b5894e|2.3.4|
|[Improve][Common] Introduce new error define rule (#5793)|https://github.com/apache/seatunnel/commit/9d1b2582b2|2.3.4|
|[Improve] Remove use `SeaTunnelSink::getConsumedType` method and mark it as deprecated (#5755)|https://github.com/apache/seatunnel/commit/8de7408100|2.3.4|
|[Improve][CheckStyle] Remove useless &#x27;SuppressWarnings&#x27; annotation of checkstyle. (#5260)|https://github.com/apache/seatunnel/commit/51c0d709ba|2.3.4|
|[Improve][pom] Formatting pom (#4761)|https://github.com/apache/seatunnel/commit/1d6d3815ec|2.3.2|
|[Hotfix][Connector-V2][RocketMQ] Fix rocketmq spark e2e test cases (#4583)|https://github.com/apache/seatunnel/commit/e711f6ef4c|2.3.2|
|[Feature][Connector-V2] Add rocketmq source and sink (#4007)|https://github.com/apache/seatunnel/commit/e333897552|2.3.2|

</details>
