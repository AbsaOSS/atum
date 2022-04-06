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

import java.nio.file.{Files, Paths}

import org.apache.log4j.LogManager
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.concurrent.Eventually
import za.co.absa.atum.AtumImplicits._

import scala.concurrent.duration.DurationInt

object SampleMeasurements1 extends Eventually {

  private val log = LogManager.getLogger(this.getClass)

  def main(args: Array[String]) {
    val sparkBuilder = SparkSession.builder().appName("Sample Measurements 1 Job")
    val spark = sparkBuilder
      // .master("local") // use this when running locally
      .getOrCreate()

    import spark.implicits._
    val hadoopConfiguration = spark.sparkContext.hadoopConfiguration
    implicit val fs: FileSystem = FileSystem.get(hadoopConfiguration)

    // Initializing library to hook up to Apache Spark
    val inputCsvInfo = this.getClass.getResource("/input/wikidata.csv.info").toString // path from test resources

    spark.enableControlMeasuresTracking(Some(inputCsvInfo), None)
      .setControlMeasuresWorkflow("Job 1")

    val inputCsv = this.getClass.getResource("/input/wikidata.csv").toString // path from test resources
    // A business logic of a spark job ...
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputCsv)
      .as("source")
      .filter($"total_response_size" > 1000)
      .setAdditionalInfo("additionalKey1", "additionalValue1")
      .setCheckpoint("checkpoint1")
      .write.mode(SaveMode.Overwrite)
      .parquet("data/output/stage1_job_results")

    eventually(timeout(scaled(10.seconds)), interval(scaled(500.millis))) {
<<<<<<< HEAD:examples/atum-examples/src/main/scala/za/co/absa/atum/examples/SampleMeasurements1.scala
      if (!Files.exists(Paths.get("data/output/stage1_job_results/_INFO"))) {
=======
      if (!fs.exists(new Path("data/output/stage1_job_results/_INFO"))) {
>>>>>>> master:examples/src/main/scala/za/co/absa/atum/examples/SampleMeasurements1.scala
        throw new Exception("_INFO file not found at data/output/stage1_job_results")
      }
    }

    spark.disableControlMeasuresTracking()
  }
}