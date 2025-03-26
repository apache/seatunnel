<details><summary> Change Log </summary>

| Change | Commit | Version |
| --- | --- | --- |
|[improve] http connector options (#8969)|https://github.com/apache/seatunnel/commit/63ff9f910|2.3.10|
|[Fix][connector-http] fix when post have param (#8434)|https://github.com/apache/seatunnel/commit/c1b2675ab|2.3.10|
|[Improve] restruct connector common options (#8634)|https://github.com/apache/seatunnel/commit/f3499a6ee|2.3.10|
|[Improve][dist]add shade check rule (#8136)|https://github.com/apache/seatunnel/commit/51ef80001|2.3.9|
|[Feature][Connector-V2] Add prometheus source and sink (#7265)|https://github.com/apache/seatunnel/commit/dde6f9fcb|2.3.9|
|[Feature][Restapi] Allow metrics information to be associated to logical plan nodes (#7786)|https://github.com/apache/seatunnel/commit/6b7c53d03|2.3.9|
|[Fix][Connector-V2] Fix http source can not read streaming (#7703)|https://github.com/apache/seatunnel/commit/a0ffa7ba0|2.3.8|
|[Feature][Connector-V2] Suport choose the start page in http paging (#7180)|https://github.com/apache/seatunnel/commit/ed15f0dcf|2.3.8|
|[Improve][Connector] Add multi-table sink option check (#7360)|https://github.com/apache/seatunnel/commit/2489f6446|2.3.7|
|[Improve][API] Make sure the table name in TablePath not be null (#7252)|https://github.com/apache/seatunnel/commit/764d8b0bc|2.3.7|
|[Feature][Core] Support using upstream table placeholders in sink options and auto replacement (#7131)|https://github.com/apache/seatunnel/commit/c4ca74122|2.3.6|
|[Feature][Kafka] Support multi-table source read  (#5992)|https://github.com/apache/seatunnel/commit/60104602d|2.3.6|
|[Improve][CDC] Close idle subtasks gorup(reader/writer) in increment phase (#6526)|https://github.com/apache/seatunnel/commit/454c339b9|2.3.6|
|Fix HttpSource bug (#6824)|https://github.com/apache/seatunnel/commit/c3ab84caa|2.3.6|
|[Hotfix] fix http source can not read yyyy-MM-dd HH:mm:ss format bug &amp; Improve DateTime Utils (#6601)|https://github.com/apache/seatunnel/commit/19888e796|2.3.5|
|[Improve][Connector-V2]Support multi-table sink feature for httpsink (#6316)|https://github.com/apache/seatunnel/commit/e6c51a95c|2.3.5|
|[Improve][HttpConnector]Increase custom configuration timeout. (#6223)|https://github.com/apache/seatunnel/commit/fa5b7d3d8|2.3.4|
|[Feature][Core] Upgrade flink source translation (#5100)|https://github.com/apache/seatunnel/commit/5aabb14a9|2.3.4|
|[BUG][Connector-V2][Http] fix bug http config no schema option and improve e2e test add case (#5939)|https://github.com/apache/seatunnel/commit/8a71b9e07|2.3.4|
|[Feature][Connector-V2] Support TableSourceFactory/TableSinkFactory on redis  (#5901)|https://github.com/apache/seatunnel/commit/e84dcb8c1|2.3.4|
|[Feature][Connector-V2] Support TableSourceFactory/TableSinkFactory on http (#5816)|https://github.com/apache/seatunnel/commit/6f49ec6ea|2.3.4|
|[Improve][Common] Introduce new error define rule (#5793)|https://github.com/apache/seatunnel/commit/9d1b2582b|2.3.4|
|[Feature][Transform] add JsonPath transform (#5632)|https://github.com/apache/seatunnel/commit/d908f0af4|2.3.4|
|[Improve] Remove use `SeaTunnelSink::getConsumedType` method and mark it as deprecated (#5755)|https://github.com/apache/seatunnel/commit/8de740810|2.3.4|
|[Feature][Connector-V2] HTTP supports page increase #5477 (#5561)|https://github.com/apache/seatunnel/commit/bb180b298|2.3.4|
|[improve][Connector-V2][http] improve http e2e test  (#5655)|https://github.com/apache/seatunnel/commit/f5867adca|2.3.4|
|Support config column/primaryKey/constraintKey in schema (#5564)|https://github.com/apache/seatunnel/commit/eac76b4e5|2.3.4|
|[BUG][Connector-V2][http] fix httpheader cover (#5446)|https://github.com/apache/seatunnel/commit/cdd8e0a65|2.3.4|
|[Feature][Connector][Http] Support multi-line text splits (#4698)|https://github.com/apache/seatunnel/commit/6a524981c|2.3.2|
|Merge branch &#x27;dev&#x27; into merge/cdc|https://github.com/apache/seatunnel/commit/4324ee191|2.3.1|
|[Improve][Project] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/423b58303|2.3.1|
|[improve][api] Refactoring schema parse (#4157)|https://github.com/apache/seatunnel/commit/b2f573a13|2.3.1|
|[Improve][build] Give the maven module a human readable name (#4114)|https://github.com/apache/seatunnel/commit/d7cd60105|2.3.1|
|[Improve][Project] Code format with spotless plugin. (#4101)|https://github.com/apache/seatunnel/commit/a2ab16656|2.3.1|
|[Feature][Connector] add get source method to all source connector (#3846)|https://github.com/apache/seatunnel/commit/417178fb8|2.3.1|
|[Hotfix] [seatunnel-connectors-v2] [connector-http] fix http json request error (#3629)|https://github.com/apache/seatunnel/commit/54f594d6c|2.3.0|
|[Improve][Connector-V2][Http]Improve json parse option rule for all http connector (#3627)|https://github.com/apache/seatunnel/commit/589e4161e|2.3.0|
|[Feature][Connector-V2][HTTP] Use json-path parsing (#3510)|https://github.com/apache/seatunnel/commit/1807eb6c9|2.3.0|
|[Improve][Connector-V2][Http]Unified exception for http source &amp; sink… (#3594)|https://github.com/apache/seatunnel/commit/d798cd867|2.3.0|
|[Hotfix][OptionRule] Fix option rule about all connectors (#3592)|https://github.com/apache/seatunnel/commit/226dc6a11|2.3.0|
|[Feature][Connector-V2][Lemlist]Add Lemlist source connector (#3346)|https://github.com/apache/seatunnel/commit/12d66b424|2.3.0|
|[Improve][Connector-V2][My Hours]Add http method enum &amp;&amp; Improve My Hours connector option rule (#3390)|https://github.com/apache/seatunnel/commit/a86c9d90f|2.3.0|
|[Feature][Connector-V2][Http] Add option rules &amp;&amp; Improve Myhours sink connector (#3351)|https://github.com/apache/seatunnel/commit/cc8bb60c8|2.3.0|
|[Feature][Connector-V2][My Hours] Add My Hours Source Connector (#3228)|https://github.com/apache/seatunnel/commit/4104a3e30|2.3.0|
|[Improve][all] change Log to @Slf4j (#3001)|https://github.com/apache/seatunnel/commit/6016100f1|2.3.0-beta|
|[Bug][format][json] Fix jackson package conflict with spark (#2934)|https://github.com/apache/seatunnel/commit/1a92b8369|2.3.0-beta|
|[Bug][Connector-V2] Fix wechat sink data serialization (#2856)|https://github.com/apache/seatunnel/commit/3aee11fc1|2.3.0-beta|
|[Improve][Connector-V2] Improve http connector (#2833)|https://github.com/apache/seatunnel/commit/5b3957bc5|2.2.0-beta|
|[DEV][Api] Replace SeaTunnelContext with JobContext and remove singleton pattern (#2706)|https://github.com/apache/seatunnel/commit/cbf82f755|2.2.0-beta|
|[Improve][build] Improved scope of maven-shade-plugin (#2665)|https://github.com/apache/seatunnel/commit/93bc8bd11|2.2.0-beta|
|[#2606]Dependency management split (#2630)|https://github.com/apache/seatunnel/commit/fc047be69|2.2.0-beta|
|[chore][connector-common] Rename SeatunnelSchema to SeaTunnelSchema (#2538)|https://github.com/apache/seatunnel/commit/7dc2a2738|2.2.0-beta|
|[Bug][Connector-V2] Fix the bug that set params by mistake (#2511) (#2513)|https://github.com/apache/seatunnel/commit/ead3d68b0|2.2.0-beta|
|[Improve][Connector-V2] Http source support user-defined schema (#2439)|https://github.com/apache/seatunnel/commit/793933b6b|2.2.0-beta|
|[Improve][Connector-V2] Format SeaTunnelRow use seatunnel-format-json (#2435)|https://github.com/apache/seatunnel/commit/e4e8f7fbf|2.2.0-beta|
|[Improve][Connector-V2] Make the attribute of http-connector from private to protected (#2418)|https://github.com/apache/seatunnel/commit/f3b00ef69|2.2.0-beta|
|[Feature][Connector-V2] Add feishu sink (#2381)|https://github.com/apache/seatunnel/commit/0fec8ca43|2.2.0-beta|

</details>
