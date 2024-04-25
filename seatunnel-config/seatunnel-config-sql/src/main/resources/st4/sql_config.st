sql_config(config) ::= <<
<config.envConfigs:{envConfig|<envConfig>}; separator="\n">

source {
<config.sourceConfigs:{sourceConfig|
  <if(sourceConfig.options)><sourceConfig.connector> {
    <sourceConfig.options:{option|<option.key> = <option.value>}; separator="\n">
  \}<endif>}; separator="\n">
}

transform {
<config.transformConfigs:{transformConfig|
  sql {
    source_table_name = "<transformConfig.sourceTableName>"
    query = """<transformConfig.query>"""
    result_table_name = "<transformConfig.resultTableName>"
  \}}; separator="\n">
}

sink {
<config.sinkConfigs:{sinkConfig|
  <if(sinkConfig.options)><sinkConfig.connector> {
    <sinkConfig.options:{option|<option.key> = <option.value>}; separator="\n">
  \}<endif>}; separator="\n">
}
>>