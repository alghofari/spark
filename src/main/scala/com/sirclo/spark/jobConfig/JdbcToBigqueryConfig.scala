package com.sirclo.spark.jobConfig

import com.fasterxml.jackson.annotation.JsonProperty

case class JdbcToBigqueryConfig(
                                  @JsonProperty("jdbc_url") dbURL:String,
                                  @JsonProperty("jdbc_credential") jdbcCredential:String,
                                  @JsonProperty("extract_query") queryReader:String,
                                  @JsonProperty("target_bq_load_method") writeLoadMethod:String,
                                  @JsonProperty("target_bq_project") targetBQProject:String,
                                  @JsonProperty("partition_field") partitionField:String,
                                  @JsonProperty("merge_query") queryWriter:String,
                                  @JsonProperty("full_target_bq_table") pathMainTable:String,
                                  @JsonProperty("full_target_bq_table_temp") pathTempTable:String,
                                  @JsonProperty("schema") schemaJson:String,
                                  @JsonProperty("source_timestamp_keys") sourceTimestampKeys:String,
                                  @JsonProperty("task_type") taskType:String
                                )
