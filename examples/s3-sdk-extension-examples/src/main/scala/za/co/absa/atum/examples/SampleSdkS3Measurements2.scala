/*
 * Copyright 2018 ABSA Group Limited
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
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider
import software.amazon.awssdk.regions.Region
import za.co.absa.atum.AtumImplicitsSdkS3._
import za.co.absa.atum.core.ControlType
import za.co.absa.atum.model.{ControlMeasure, Measurement}
import za.co.absa.atum.persistence.s3.{ControlMeasuresSdkS3LoaderJsonFile, S3KmsSettings, SimpleS3LocationWithRegion}
import za.co.absa.atum.utils.SdkS3ClientUtils

object SampleSdkS3Measurements2 {
  val log: Logger = LogManager.getLogger("SampleSdkS3Measurements2")

  def main(args: Array[String]) {

    // This example is intended to run AFTER SampleMeasurements1, otherwise it will fail on input file absence

    val sparkBuilder = SparkSession.builder().appName("Sample Measurements 2 Job")
    val spark = sparkBuilder
      // .master("local") // use this when running locally
      .getOrCreate()

    import spark.implicits._

    val hadoopConfiguration = spark.sparkContext.hadoopConfiguration
    implicit val fs: FileSystem = FileSystem.get(hadoopConfiguration)

    // This sample example relies on local credentials profile named "saml" with access to the s3 location defined below
    // AND by having explicitly defined KMS Key ID
    implicit val samlCredentialsProvider: ProfileCredentialsProvider = SdkS3ClientUtils.getLocalProfileCredentialsProvider("saml")
    val kmsKeyId = System.getenv("TOOLING_KMS_KEY_ID") // load from an environment property in order not to disclose it here
    val myBucket = System.getenv("TOOLING_BUCKET_NAME") // same reason
    log.info(s"kmsKeyId from env loaded = ${kmsKeyId.take(10)}...")
    log.info(s"BucketName from env loaded = $myBucket")

    val infoFileOutputLocation = SimpleS3LocationWithRegion("s3", myBucket, "atum/output/wikidata.csv.info", Region.EU_WEST_1)

    // Initializing library to hook up to Apache Spark
    // No need to specify datasetName and datasetVersion as it is stage 2 and it will be determined automatically
    spark.enableControlMeasuresTrackingForSdkS3(
      sourceS3Location = None,
      destinationS3Config = Some(
        infoFileOutputLocation,
        S3KmsSettings(kmsKeyId)
      )
    ).setControlMeasuresWorkflow("Job 2")

    val sourceDS = spark.read
      .parquet("data/output_s3/stage1_job_results")

    // A business logic of a spark job ...

    // An example - a column rename
    // If the renamed column is one of control measurement columns, the rename need to be registered in Control Framework
    sourceDS.as("target")
      .withColumnRenamed("total_response_size", "trs") // Renaming the column
      .registerColumnRename("total_response_size", "trs") // Registering the rename, from now on the new name for the column is 'trs'
      .filter($"trs" > 15000)
      .setCheckpoint("checkpoint2")
      .write.mode(SaveMode.Overwrite)
      .parquet("data/output_s3/stage2_job_results")

    spark.disableControlMeasuresTracking()

    // checking info file presence via wrapped AWS SDK s3
    val loader = ControlMeasuresSdkS3LoaderJsonFile(infoFileOutputLocation)
    val controlMeasure = loader.load()

    val expectedCheckpointRecordCounts = Seq(
      "Source" -> 4964, "Raw" -> 4964, "checkpoint1" -> 3072, "checkpoint2" -> 1651)
    val extractedCounts = extractCheckpointsRecordCounts(controlMeasure)
    assert(extractedCounts == expectedCheckpointRecordCounts, s"expecting control measure counts to be: $expectedCheckpointRecordCounts, but $extractedCounts found.")
  }

  private def extractCheckpointsRecordCounts(controlMeasure: ControlMeasure): Seq[(String, Int)] = {
    controlMeasure.checkpoints.map { checkpoint =>
      val count: Int = checkpoint.controls.collectFirst { case Measurement(ControlType.Count.value, "*", value) => value.toString.toInt }.getOrElse(0)
      (checkpoint.name, count)
    }
  }
}
