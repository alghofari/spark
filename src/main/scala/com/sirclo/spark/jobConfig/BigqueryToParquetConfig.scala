package com.sirclo.spark.jobConfig
import com.fasterxml.jackson.annotation.JsonProperty

case class BigqueryToParquetConfig(
                                     @JsonProperty("extract_query") queryReader:String,
                                     @JsonProperty("bq_project") BQProject:String,
                                     @JsonProperty("bq_dataset") tempDataset:String,
                                     @JsonProperty("hive_partition_key") hivePartitionKey:String,
                                     @JsonProperty("gcs_folder") GCSFolder:String,
                                     @JsonProperty("gcs_bucket") GCSBucket:String,
                                     @JsonProperty("gcs_project") GCSProject:String,
                                   )
