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

package za.co.absa.atum.integration

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode
import org.scalatest.{BeforeAndAfterAll, OptionValues}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.atum.AtumImplicits._
import za.co.absa.atum.integration.SampleMeasurementsIntegrationSuite.TestBuildProperties
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.atum.utils.{BuildProperties, FileUtils, SerializationUtils, SparkTestBase}

import scala.concurrent.duration.DurationInt

object SampleMeasurementsIntegrationSuite {

  private case class TestBuildProperties(projectName: String, buildVersion: String) extends BuildProperties

}

class SampleMeasurementsIntegrationSuite extends AnyFlatSpec with Matchers with Eventually with SparkTestBase
  with BeforeAndAfterAll with OptionValues {

  implicit val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  import spark.implicits._ // $"interpolation"

  val outputPath = "src/test/resources/data/output"

  override def beforeAll() {
    fs.delete(new Path("data/output"), true)
  }

  override def afterAll() {
    fs.delete(new Path("data/output"), true)
  }

  "SampleMeasurementIntegTest" should "load explicit _INFO, adjust, and save implicit _INFO (stage 1)" in {
    spark.enableControlMeasuresTracking(Some(getClass.getResource(s"/data/input/wikidata.csv.info").getPath), None)
      .setControlMeasuresWorkflow("Job 1")

    // Some business logic of a spark job ...
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource(s"/data/input/wikidata.csv").getPath)
      .as("source")
      .filter($"total_response_size" > 1000)
      .setAdditionalInfo("additionalKey1", "additionalValue1")
      .setCheckpoint("checkpoint1")
      .write.mode(SaveMode.Overwrite)
      .parquet("data/output/stage1_job_results")

    // assert the state after
    eventually(timeout(scaled(10.seconds)), interval(scaled(500.millis))) {
      fs.exists(new Path(s"data/output/stage1_job_results/_INFO")) shouldBe true

      val outputData = FileUtils.readFileToString(s"data/output/stage1_job_results/_INFO")
      val outputControlMeasure: ControlMeasure = SerializationUtils.fromJson[ControlMeasure](outputData)

      outputControlMeasure.metadata.additionalInfo should contain("additionalKey1", "additionalValue1")
      outputControlMeasure.checkpoints.map(_.name) should contain theSameElementsInOrderAs Seq("Source", "Raw", "checkpoint1")
    }

    spark.disableControlMeasuresTracking()
  }

  it should "load implicit _INFO, adjust, and save implicit _INFO (stage 2)" in {
    // Demonstrating that the default _INFO input and output will be determined automatically
    spark.enableControlMeasuresTracking(None, None, TestBuildProperties("MySoftware", "v007")) // custom sw+version
      .setControlMeasuresWorkflow("Job 2")

    val sourceDS = spark.read
      .parquet(s"data/output/stage1_job_results") // stage1_output as input for stage 2

    // Some business logic of a spark job ...
    // An example - a column rename
    // If the renamed column is one of control measurement columns, the rename need to be registered in Control Framework
    sourceDS.as("target")
      .withColumnRenamed("total_response_size", "trs") // Renaming the column
      .registerColumnRename("total_response_size", "trs") // Registering the rename, from now on the new name for the column is 'trs'
      .filter($"trs" > 1000)
      .setCheckpoint("checkpoint2")
      .write.mode(SaveMode.Overwrite)
      .parquet("data/output/stage2_job_results")

    // assert the state after
    eventually(timeout(scaled(10.seconds)), interval(scaled(500.millis))) {
      fs.exists(new Path(s"data/output/stage2_job_results/_INFO")) shouldBe true


      val outputData = FileUtils.readFileToString(s"data/output/stage2_job_results/_INFO")
      val outputControlMeasure: ControlMeasure = SerializationUtils.fromJson[ControlMeasure](outputData)

      outputControlMeasure.checkpoints.map(_.name) should contain theSameElementsInOrderAs
        Seq("Source", "Raw", "checkpoint1", "checkpoint2")

      val checkpoint1 = outputControlMeasure.checkpoints.filter(_.name == "checkpoint1").head // from stage1
      val checkpoint2 = outputControlMeasure.checkpoints.filter(_.name == "checkpoint2").head // now added

      checkpoint1.software.value should startWith("atum") // default software
      checkpoint1.version should not be empty // some default version should be present
      checkpoint2.software.value shouldBe "MySoftware" // custom BuildProperties taking effect
      checkpoint2.version.value shouldBe "v007"

      // showing that "total_response_size" got renamed to "trs"
      checkpoint1.controls.map(_.controlCol) should contain("total_response_size")
      checkpoint2.controls.map(_.controlCol) should (contain("trs") and not contain ("total_response_size"))
    }

    spark.disableControlMeasuresTracking()
  }
}
