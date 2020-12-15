/*
 * Copyright 2018-2019 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.atum.examples

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{SaveMode, SparkSession}
import software.amazon.awssdk.regions.Region
import za.co.absa.atum.AtumImplicitsSdkS3._
import za.co.absa.atum.AtumImplicits._
import za.co.absa.atum.core.AtumSdkS3
import za.co.absa.atum.persistence.SimpleS3LocationWithRegion
import za.co.absa.atum.utils.SdkS3ClientUtils

object SampleSdkS3Measurements1 {
  def main(args: Array[String]) {
    val sparkBuilder = SparkSession.builder().appName("Sample S3 Measurements 1 Job")
    val spark = sparkBuilder
      // .master("local")
      .getOrCreate()

    import spark.implicits._

    val hadoopConfiguration = spark.sparkContext.hadoopConfiguration
    implicit val fs = FileSystem.get(hadoopConfiguration)
    implicit val atum = AtumSdkS3 // using extended Atum for SdkS3

    // This sample example relies on local credentials profile named "saml" with access to the s3 location defined below
    implicit val samlCredentialsProvider = SdkS3ClientUtils.getLocalProfileCredentialsProvider("saml")
    val myBucket = System.getenv("TOOLING_BUCKET_NAME") // load from an environment property in order not to disclose it here

    // Initializing library to hook up to Apache Spark
    spark.enableControlMeasuresTrackingForSdkS3(
      sourceS3Location = Some(SimpleS3LocationWithRegion("s3", myBucket, "atum/input/wikidata.csv.info", Region.EU_WEST_1)),
      destinationS3Config = None
    ).setControlMeasuresWorkflow("Job 1 S3 ")

    // A business logic of a spark job ...

    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/input/wikidata.csv")
      .as("source")
      .filter($"total_response_size" > 10000)
      .setCheckpoint("checkpoint1")
      .write.mode(SaveMode.Overwrite)
      .parquet("data/output_s3/stage1_job_results")

    spark.disableControlMeasuresTracking()
  }
}
