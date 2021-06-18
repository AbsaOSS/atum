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

package za.co.absa.atum

import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.atum.AtumImplicits._
import za.co.absa.atum.model.{Checkpoint, Measurement}
import za.co.absa.atum.persistence.ControlMeasuresParser
import za.co.absa.atum.utils.SparkTestBase

import java.nio.file.{Files, Paths}
import scala.concurrent.duration.DurationInt

class HdfsInfoIntegrationSuite extends AnyFlatSpec with SparkTestBase with Matchers with BeforeAndAfterAll with Eventually {

  private val log = LogManager.getLogger(this.getClass)
  val tempDir: String = LocalFsTestUtils.createLocalTemporaryDirectory("hdfsTestOutput")

  override def afterAll: Unit = {
    LocalFsTestUtils.safeDeleteTestDir(tempDir)
  }

  private val inputCsv = "data/input/wikidata.csv"
  private def readSparkInputCsv(inputCsvPath: String): DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(inputCsvPath)

  private def writeSparkData(df: DataFrame, outputPath: String): Unit = {
    df.write.mode(SaveMode.Overwrite)
      .parquet(outputPath)

    eventually(timeout(scaled(10.seconds)), interval(scaled(500.millis))) {
      if (!Files.exists(Paths.get(outputPath)))
        throw new Exception("_INFO file not found at " + outputPath)
    }
  }

  val outputPath = s"$tempDir/outputCheck1"

  "_INFO" should s"be written on spark.write (implicit output _INFO path only)" in {
    val expectedPath = s"$outputPath/_INFO"
    val hadoopConfiguration = spark.sparkContext.hadoopConfiguration
    implicit val fs: FileSystem = FileSystem.get(hadoopConfiguration)

    synchronized {
      // Initializing library to hook up to Apache Spark
      spark.enableControlMeasuresTracking(sourceInfoFilePath = Some("data/input/wikidata.csv.info"), destinationInfoFilePath = None)
        .setControlMeasuresWorkflow("Job 1")

      import spark.implicits._
      val df1 = readSparkInputCsv(inputCsv)
      df1.setCheckpoint("Checkpoint0")
      val filteredDf1 = df1.filter($"total_response_size" > 1000)
      filteredDf1.setCheckpoint("Checkpoint1") // stateful, do not need return value
      writeSparkData(filteredDf1, outputPath) // implicit output _INFO file path is derived from this path passed to spark.write

      spark.disableControlMeasuresTracking()
    }

    log.info(s"Checking $expectedPath to contain expected values")

    val infoControlMeasures =  eventually(timeout(scaled(10.seconds)), interval(scaled(2.seconds))) {
      log.info(s"Reading $expectedPath")
      val infoContentJson = LocalFsTestUtils.readFileAsString(expectedPath)
      ControlMeasuresParser.fromJson(infoContentJson)
    }

    infoControlMeasures.checkpoints.map(_.name) shouldBe Seq("Source", "Raw", "Checkpoint0", "Checkpoint1")
    val checkpoint0 = infoControlMeasures.checkpoints.collectFirst { case c: Checkpoint if c.name == "Checkpoint0" => c }.get
    checkpoint0.controls should contain(Measurement("recordCount", "count", "*", "5000"))

    val checkpoint1 = infoControlMeasures.checkpoints.collectFirst { case c: Checkpoint if c.name == "Checkpoint1" => c }.get
    checkpoint1.controls should contain(Measurement("recordCount", "count", "*", "4964"))
  }

  "_INFO" should s"be written on spark.write (implicit & explicit output _INFO path)" in {
    val hadoopConfiguration = spark.sparkContext.hadoopConfiguration
    implicit val fs: FileSystem = FileSystem.get(hadoopConfiguration)

    synchronized {
      // Initializing library to hook up to Apache Spark
      spark.enableControlMeasuresTracking(sourceInfoFilePath = Some("data/input/wikidata.csv.info"), destinationInfoFilePath = Some(s"$outputPath/extra/_INFO2"))
        .setControlMeasuresWorkflow("Job 1")

      import spark.implicits._
      val df1 = readSparkInputCsv(inputCsv)
      df1.setCheckpoint("Checkpoint0")
      val filteredDf1 = df1.filter($"total_response_size" > 1000)
      filteredDf1.setCheckpoint("Checkpoint1") // stateful, do not need return value
      writeSparkData(filteredDf1, outputPath) // implicit output _INFO file path is derived from this path passed to spark.write

      spark.disableControlMeasuresTracking()
    }

    Seq(s"$outputPath/_INFO", s"$outputPath/extra/_INFO2").foreach { expectedPath =>
      log.info(s"Checking $expectedPath to contain expected values")

      val infoControlMeasures =  eventually(timeout(scaled(10.seconds)), interval(scaled(2.seconds))) {
        log.info(s"Reading $expectedPath")
        val infoContentJson = LocalFsTestUtils.readFileAsString(expectedPath)
        ControlMeasuresParser.fromJson(infoContentJson)
      }

      infoControlMeasures.checkpoints.map(_.name) shouldBe Seq("Source", "Raw", "Checkpoint0", "Checkpoint1")
      val checkpoint0 = infoControlMeasures.checkpoints.collectFirst { case c: Checkpoint if c.name == "Checkpoint0" => c }.get
      checkpoint0.controls should contain(Measurement("recordCount", "count", "*", "5000"))

      val checkpoint1 = infoControlMeasures.checkpoints.collectFirst { case c: Checkpoint if c.name == "Checkpoint1" => c }.get
      checkpoint1.controls should contain(Measurement("recordCount", "count", "*", "4964"))
    }
  }

}
