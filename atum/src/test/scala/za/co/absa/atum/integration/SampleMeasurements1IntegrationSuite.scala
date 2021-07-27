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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.atum.AtumImplicits._
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.atum.utils.{FileUtils, SerializationUtils, SparkTestBase}

import scala.concurrent.duration.DurationInt

class SampleMeasurements1IntegrationSuite extends AnyFlatSpec with Matchers with Eventually with SparkTestBase {

  val inputPath = "src/test/resources/data/input"
  val outputPath = "src/test/resources/data/output"

  implicit val fs: FileSystem = FileSystem.get(new Configuration())

  "SampleMeasurementIntegTest" should "correctly load, adjust and implicit save _INFO" in {

    import spark.implicits._
    val hadoopConfiguration = spark.sparkContext.hadoopConfiguration
    implicit val fs: FileSystem = FileSystem.get(hadoopConfiguration)

    // Initializing library to hook up to Apache Spark
    spark.enableControlMeasuresTracking(Some(s"$inputPath/wikidata.csv.info"), None)
      .setControlMeasuresWorkflow("Job 1")

    // A business logic of a spark job ...

    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"$inputPath/wikidata.csv")
      .as("source")
      .filter($"total_response_size" > 1000)
      .setAdditionalInfo(("additionalKey1", "additionalValue1"))
      .setCheckpoint("checkpoint1")
      .write.mode(SaveMode.Overwrite)
      .parquet(s"$outputPath/stage1_job_results")

    eventually(timeout(scaled(10.seconds)), interval(scaled(500.millis))) {
      fs.exists(new Path((s"$outputPath/stage1_job_results/_INFO"))) shouldBe true
    }

    val outputData = FileUtils.readFileToString(s"$outputPath/stage1_job_results/_INFO")
    val outputControlMeasure: ControlMeasure = SerializationUtils.fromJson[ControlMeasure](outputData)

    // assert the state after
    outputControlMeasure.metadata.additionalInfo should contain("additionalKey1", "additionalValue1")
    outputControlMeasure.checkpoints.map(_.name) should contain theSameElementsInOrderAs (Seq("Source", "Raw", "checkpoint1"))

    spark.disableControlMeasuresTracking()

    fs.delete(new Path(s"$outputPath/stage1_job_results"), true)
  }
}
