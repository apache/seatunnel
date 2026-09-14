<details><summary> Change Log </summary>

| Change | Commit | Version |
| --- | --- | --- |
|[Fix][CDC][Zeta] Restore runtime schema from checkpoint after failover (#11503)|https://github.com/apache/seatunnel/commit/ec1b1b8b5|3.0.0|
|[Improve][Connector-V2][File] Validate anydoc Markdown handoff (#11813)|https://github.com/apache/seatunnel/commit/2531b4ef0|3.0.0|
|[Feature][Connector-V2] Add Google Cloud Storage file sink (#12146)|https://github.com/apache/seatunnel/commit/72acda570|3.0.0|
|[Fix][Connector-V2] Fix file content comparison across read boundaries (#12080)|https://github.com/apache/seatunnel/commit/2f67a4d94|3.0.0|
|[Feature][Connector-V2] Add Google Cloud Storage file source (#11967)|https://github.com/apache/seatunnel/commit/96d8e8d39|3.0.0|
|[Feature][Connector-V2] Add BosFile source and sink connector (#11952)|https://github.com/apache/seatunnel/commit/99e53aad8|3.0.0|
|[Improve][Connector-V2] Align Markdown RAG metadata with Knowledge Sync (#11740)|https://github.com/apache/seatunnel/commit/9a1b6b4e6|3.0.0|
|[Fix][Connector-V2][File] Fix Parquet INT96 writes for uppercase fields (#10943)|https://github.com/apache/seatunnel/commit/406c66789|3.0.0|
|[Feature][shade]Refactor the seatunnel-shade module. (#9993)|https://github.com/apache/seatunnel/commit/4ba289595|3.0.0|
|[Fix][Connector-V2] Preserve ORC nested field case (#11428)|https://github.com/apache/seatunnel/commit/99c7295ff|3.0.0|
|[Fix][Connector-V2] Close Parquet and ORC writers when rolling files (#11832)|https://github.com/apache/seatunnel/commit/ce196d8fa|3.0.0|
|[Fix][Connector-V2] Harden XML file parsing against XXE (#11250)|https://github.com/apache/seatunnel/commit/7b40dd1ac|3.0.0|
|[Fix][Connector-V2] Remove unreachable LocalTimestampMillisConversion in ParquetWriteStrategy|https://github.com/apache/seatunnel/commit/9b3deb9ed|3.0.0|
|[Improve][Connector-V2][File] Parse the Hadoop configuration once per subtask instead of once per output file (#11661)|https://github.com/apache/seatunnel/commit/deb1622e2|3.0.0|
|[Improve][Connector-V2] Guard POI Excel reads by file size (#11591)|https://github.com/apache/seatunnel/commit/261644ff4|3.0.0|
|[Feature][Connector-File-Base] Add optional PDF RAG metadata for file source (#11571)|https://github.com/apache/seatunnel/commit/2b060343d|3.0.0|
|[Fix][Connector-V2][File] Close the GZ input stream in AbstractReadStrategy (#10532)|https://github.com/apache/seatunnel/commit/ac8306f33|3.0.0|
|[Improve][Connector-V2] Route Markdown file source splits by document id (#10964)|https://github.com/apache/seatunnel/commit/2091c6e8a|3.0.0|
|[Feature][Connector-V2] Add checkpoint-gated post_sync_action and retention for file source continuous discovery (#10563)|https://github.com/apache/seatunnel/commit/3d4aa03a5|3.0.0|
|[Improve][Connector-V2][File] Optimize file discovery and update comparison for sync_mode=update (#11312)|https://github.com/apache/seatunnel/commit/6de6f1b95|3.0.0|
|[Improve][Connector-V2][SftpFile] Defer binary file discovery (#11377)|https://github.com/apache/seatunnel/commit/886c0fd13|3.0.0|
|[Feature][File] Add pdf parser for RAG support (#10105)|https://github.com/apache/seatunnel/commit/b6cdd2408|3.0.0|
|[Fix][Connector-V2] Fix Parquet INT96 mixed-case field matching (#11067)|https://github.com/apache/seatunnel/commit/b38601a07|3.0.0|
|[Feature][Connector-V2] Add recursive_file_scan option for file connectors (#10505)|https://github.com/apache/seatunnel/commit/2826ea3dc|3.0.0|
|[Fix][Connector-V2] Ignore BOM in file source readers (#11056)|https://github.com/apache/seatunnel/commit/ba53e82ba|3.0.0|
|[Fix][Connector-file] Fix the new schema cannot be fetched when the parquet file is read (#10378)|https://github.com/apache/seatunnel/commit/b0b095eb4|3.0.0|
|[Feature][Connector-File] Add schema evolution support (ADD/DROP/RENAME/UPDATE column) for all file formats (#10744)|https://github.com/apache/seatunnel/commit/e592175b5|3.0.0|
|[Fix][Connector-V2] Fix parquet read failure when column name contains Avro-illegal characters (#10960)|https://github.com/apache/seatunnel/commit/8ef362f84|3.0.0|
|[Feature][Connector-V2] Add optional Markdown RAG metadata for file source (#10844)|https://github.com/apache/seatunnel/commit/970cadb1a|3.0.0|
|[Fix][Connector-V2][File] Respect custom filename for binary sink (#10817)|https://github.com/apache/seatunnel/commit/72f32edde|3.0.0|
|[Improve] File souce refactor  (#10758)|https://github.com/apache/seatunnel/commit/bf2529256|3.0.0|
|[Fix][Connector-V2] Clean up file sink transaction parent dirs (#10815)|https://github.com/apache/seatunnel/commit/527e2e31f|3.0.0|
|[Fix][Connector-V2] Fix ORC File Source corrupts BINARY type data (#10820)|https://github.com/apache/seatunnel/commit/2ee0ac490|3.0.0|
|[Improve][Connector-v2][File] error handling for file and directory operations in HadoopFileSystemProxy (#10433)|https://github.com/apache/seatunnel/commit/52bf50cad|3.0.0|
|[Feature] Support define any nested array and map type with schema config (#10396)|https://github.com/apache/seatunnel/commit/2a2993309|3.0.0|
|[Fix][Connector-V2][File] Fix Excel write order issues (#10365) (#10366)|https://github.com/apache/seatunnel/commit/0bc6b3406|3.0.0|
|[Fix][Zeta] prevent cancel stuck and downgrade tmp cleanup failure (#10729)|https://github.com/apache/seatunnel/commit/2c81f7f3c|3.0.0|
|[Improve][Connectors-v2] [ORC]parse config on file level rather than field level (#10759)|https://github.com/apache/seatunnel/commit/5111281af|3.0.0|
|[Feature][Connector-V2] Add continuous discovery for FTP/SFTP/Local/HDFS file sources (#10473)|https://github.com/apache/seatunnel/commit/01cd08abc|3.0.0|
|[Improve][Connectors-v2] File sink refactor (#10587)|https://github.com/apache/seatunnel/commit/efeed28ae|3.0.0|
|[Feature][Connector-V2] Enable file split for S3File source (#10450)|https://github.com/apache/seatunnel/commit/37999ea5b|3.0.0|
|[Feature][seatunnel-api] Integrate Gravitino as metadata service for non-relational connectors (#10402)|https://github.com/apache/seatunnel/commit/e24b8c140|3.0.0|

</details>
