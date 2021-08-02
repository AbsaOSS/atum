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

import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.concurrent.Eventually
import za.co.absa.atum.AtumImplicits._
import za.co.absa.atum.examples.SampleMeasurements2.{eventually, interval, scaled, timeout}
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.atum.utils.{BuildProperties, FileUtils, SerializationUtils}

import scala.concurrent.duration.DurationInt

object SampleMeasurements3 extends Eventually {
  case class MyBuildProperties(projectName: String, buildVersion: String) extends BuildProperties

  private val log = LogManager.getLogger(this.getClass)

  def main(args: Array[String]) {
    val sparkBuilder = SparkSession.builder().appName("Sample Measurements 3 Job")
    val spark = sparkBuilder
      // .master("local") // use this when running locally
      .getOrCreate()

    import spark.implicits._
    val hadoopConfiguration = spark.sparkContext.hadoopConfiguration
    implicit val fs: FileSystem = FileSystem.get(hadoopConfiguration)

    // Initializing library to hook up to Apache Spark
    spark.enableControlMeasuresTracking(Some("data/input/wikidata.csv.info"), None, MyBuildProperties("MySoftware", "v007"))
      .setControlMeasuresWorkflow("Job 1")

    // A business logic of a spark job ...
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/input/wikidata.csv")
      .as("source")
      .filter($"total_response_size" > 1000)
      .setCheckpoint("checkpoint1")
      .write.mode(SaveMode.Overwrite)
      .parquet("data/output/stage3_job_results")

    spark.disableControlMeasuresTracking()
    spark.close()

    eventually(timeout(scaled(10.seconds)), interval(scaled(500.millis))) { // scaling will help on slow environments
      if (!Files.exists(Paths.get("data/output/stage3_job_results/_INFO"))) {
        throw new Exception("_INFO file not found at data/output/stage3_job_results")
      }

      val jsonInfoFile = FileUtils.readFileToString("data/output/stage3_job_results/_INFO")
      val measureObject1: ControlMeasure = SerializationUtils.fromJson[ControlMeasure](jsonInfoFile)
      val checkpoint = measureObject1.checkpoints.filter(_.name == "checkpoint1").head

      if (!checkpoint.software.contains("MySoftware") || !checkpoint.version.contains("v007")) {
        throw new Exception(s"Software or Version was not set properly. Got name ${checkpoint.software} and version ${checkpoint.version}")
      } else {
        log.info("_INFO file correctly contained custom SW Name and version.")
      }

    }
  }
}
