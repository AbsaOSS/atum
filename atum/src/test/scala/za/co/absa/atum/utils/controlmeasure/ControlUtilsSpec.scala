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

package za.co.absa.atum.utils.controlmeasure

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.atum.model.{Checkpoint, ControlMeasure}
import za.co.absa.atum.utils.{HdfsFileUtils, SparkTestBase}

object ControlUtilsSpec { // todo move this?
  val testingVersion = "1.2.3"
  val testingDate = "20-02-2020"
  val testingDateTime1 = "20-02-2020 10:20:30 +0100"
  val testingDateTime2 = "20-02-2020 10:20:40 +0100"

  /**
   * Replaces metadata.informationDate, checkpoints.[].{version, processStartTime, processEndTime} with ControlUtilsSpec.testing* values
   * (and replaces CRLF endings with LF if found, too) in JSON (regarless
   *
   * @param actualJson
   * @return updated json
   */
  def stabilizeJsonOutput(actualJson: String): String = {
    actualJson
      .replaceFirst("""(?<="informationDate"\s?:\s?")(\d{2}-\d{2}-\d{4})""", testingDate)
      .replaceAll("""(?<="processStartTime"\s?:\s?")([-+: \d]+)""", testingDateTime1)
      .replaceAll("""(?<="processEndTime"\s?:\s?")([-+: \d]+)""", testingDateTime2)
      .replaceAll("""(?<="version"\s?:\s?")([-\d\.A-z]+)""", testingVersion)
      .replaceAll("\r\n", "\n") // Windows guard
  }

  implicit class ControlMeasureStabilizationExt(cm: ControlMeasure) {
    def replaceInformationDate(newDate: String): ControlMeasure = cm.copy(metadata = cm.metadata.copy(informationDate = newDate))

    def updateCheckpoints(fn: Checkpoint => Checkpoint): ControlMeasure = cm.copy(checkpoints = cm.checkpoints.map(fn))

    def replaceCheckpointsVersion(newVersion: Option[String]): ControlMeasure = cm.updateCheckpoints(_.copy(version = newVersion))
    def replaceCheckpointsProcessStartTime(newDateTime: String): ControlMeasure = cm.updateCheckpoints(_.copy(processStartTime = newDateTime))
    def replaceCheckpointsProcessEndTime(newDateTime: String): ControlMeasure = cm.updateCheckpoints(_.copy(processEndTime = newDateTime))

    def stabilizeTestingControlMeasure: ControlMeasure = {
      cm.replaceInformationDate(testingDate)
        .replaceCheckpointsVersion(Some(testingVersion))
        .replaceCheckpointsProcessStartTime(testingDateTime1)
        .replaceCheckpointsProcessEndTime(testingDateTime2)
    }
  }

}

class ControlUtilsSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  import ControlUtilsSpec._
  import spark.implicits._

  private val singleStringColumnDF = spark.sparkContext.parallelize(List("987987", "example", "example", "another example")).toDF
  private val singleStringColumnDF2 = spark.sparkContext.parallelize(List("987987", "another example")).toDF

  private val singleIntColumnDF = spark.sparkContext.parallelize(List(100, 10000, -10000, 999)).toDF
  private val singleIntColumnDF2 = spark.sparkContext.parallelize(List(100, 999)).toDF

  private val expectedJsonForSingleIntColumn =
    s"""{"metadata":{"sourceApplication":"Test","country":"ZA","historyType":"Snapshot","dataFilename":"_testOutput/data",
       |"sourceType":"Source","version":1,"informationDate":"$testingDate","additionalInfo":{}},
       |"checkpoints":[{"name":"Source","software":"Atum","version":"$testingVersion","processStartTime":"$testingDateTime1",
       |"processEndTime":"$testingDateTime2","workflowName":"Source","order":1,
       |"controls":[{"controlName":"recordCount","controlType":"count","controlCol":"*","controlValue":"4"},
       |{"controlName":"valueControlTotal","controlType":"absAggregatedTotal","controlCol":"value","controlValue":"21099"}]}]}""".stripMargin.replaceAll("\n", "")

  private val expectedPrettyJsonForSingleIntColumn =
    s"""{
       |  "metadata" : {
       |    "sourceApplication" : "Test",
       |    "country" : "ZA",
       |    "historyType" : "Snapshot",
       |    "dataFilename" : "_testOutput/data",
       |    "sourceType" : "Source",
       |    "version" : 1,
       |    "informationDate" : "$testingDate",
       |    "additionalInfo" : { }
       |  },
       |  "checkpoints" : [ {
       |    "name" : "Source",
       |    "software" : "Atum",
       |    "version" : "$testingVersion",
       |    "processStartTime" : "$testingDateTime1",
       |    "processEndTime" : "$testingDateTime2",
       |    "workflowName" : "Source",
       |    "order" : 1,
       |    "controls" : [ {
       |      "controlName" : "recordCount",
       |      "controlType" : "count",
       |      "controlCol" : "*",
       |      "controlValue" : "4"
       |    }, {
       |      "controlName" : "valueControlTotal",
       |      "controlType" : "absAggregatedTotal",
       |      "controlCol" : "value",
       |      "controlValue" : "21099"
       |    } ]
       |  } ]
       |}""".stripMargin

  Seq(
    ("as json", false, expectedJsonForSingleIntColumn),
    ("as pretty json", true, expectedPrettyJsonForSingleIntColumn)
  ).foreach { case (testCaseName, isJsonPretty, expectedMeasureJson) =>
    "createInfoFile" should s"generates a complete info file and file content $testCaseName" in {

      val hadoopConf = spark.sparkContext.hadoopConfiguration
      implicit val hdfs = FileSystem.get(hadoopConf)
      hdfs.delete(new Path("_testOutput/data/_INFO"), false) // cleanup if exists

      val actual = ControlUtils.createInfoFile(singleIntColumnDF, "Test", "_testOutput/data", writeToHDFS = true, prettyJSON = isJsonPretty, aggregateColumns = Seq("value"))
      // replace non-stable fields (date/time, version) using rx lookbehind
      val actualStabilized = stabilizeJsonOutput(actual)

      // testing the generated jsons
      assert(actualStabilized == expectedMeasureJson, "Generated json does not match")

      // check the what's actually written to "HDFS"
      val actual2 = HdfsFileUtils.readHdfsFileToString(new Path("_testOutput/data/_INFO"))
      assert(stabilizeJsonOutput(actual2) == expectedMeasureJson, "json written to _testOutput/data/_INFO")

      hdfs.deleteOnExit(new Path("_testOutput")) // eventual cleanup
    }
  }

  "createInfoFile" should "handle integer columns" in {
    val expected = "{\"controlName\":\"valueControlTotal\",\"controlType\":\"absAggregatedTotal\",\"controlCol\":\"value\",\"controlValue\":\"21099\"}]}]}"

    val actual = ControlUtils.createInfoFile(singleIntColumnDF, "Test", "/data", writeToHDFS = false, prettyJSON = false, aggregateColumns = Seq("value"))

    assert(actual.contains(expected))
  }

  "createInfoFile" should "handle numeric values cancellations" in {
    // This test handles cases when wrong aggregator is used for numeric values.
    // For example, if SUM() is used as an aggregator opposite values will cancel each other and
    // the final control value will be the same for datasets containing two values and for a dataset containing none at all
    val matcher = ".*\"controlValue\":\"(\\d+)\".*".r
    val json1 = ControlUtils.createInfoFile(singleIntColumnDF, "Test", "/data", writeToHDFS = false, prettyJSON = false, aggregateColumns = Seq("value"))
    val json2 = ControlUtils.createInfoFile(singleIntColumnDF2, "Test", "/data", writeToHDFS = false, prettyJSON = false, aggregateColumns = Seq("value"))

    val matcher(value1) = json1
    val matcher(value2) = json2

    // control values generated for these 2 datasets should not be the same
    assert(value1 != value2)
  }

  "createInfoFile" should "handle string columns" in {
    val expected = "{\"controlName\":\"valueControlTotal\",\"controlType\":\"hashCrc32\",\"controlCol\":\"value\",\"controlValue\":\"9483370936\"}]}]}"

    val actual = ControlUtils.createInfoFile(singleStringColumnDF, "Test", "/data", writeToHDFS = false, prettyJSON = false, aggregateColumns = Seq("value"))

    assert(actual.contains(expected))
  }

  "createInfoFile" should "handle string hash cancellations" in {
    // This test handles cases when wrong aggregator is used for string hash values.
    // For example, if XOR is used as an aggregator duplicate strings will cancel each other and
    // the final hash will be the same for datasets containing a duplicate values and containing no such values at all

    val matcher = ".*\"controlValue\":\"(\\d+)\".*".r
    val json1 = ControlUtils.createInfoFile(singleStringColumnDF, "Test", "/data", writeToHDFS = false, prettyJSON = false, aggregateColumns = Seq("value"))
    val json2 = ControlUtils.createInfoFile(singleStringColumnDF2, "Test", "/data", writeToHDFS = false, prettyJSON = false, aggregateColumns = Seq("value"))

    val matcher(value1) = json1
    val matcher(value2) = json2

    // control values generated for these 2 datasets should not be the same
    assert(value1 != value2)
  }

  "getTemporaryColumnName" should "generate a temporary column name" in {
    val inputDataJson = spark.sparkContext.parallelize(
      s"""{"id": 1, "name": "Foxes", "price": 100.12 } """ ::
        s"""{"id": 2, "name": "Owls", "price": 200.55 } """ :: Nil)

    val schema = StructType(
      Array(
        StructField("id", LongType, nullable = false),
        StructField("name", StringType),
        StructField("price", DecimalType(10, 6))
      ))

    val df = spark.read
      .schema(schema)
      .json(inputDataJson.toDS)

    val matcher = "(tmp_\\d+)".r
    val colName = ControlUtils.getTemporaryColumnName(df)

    colName match {
      case matcher(_) =>
      case _ => fail(s"A temporaty column name '$colName' doesn't match the required pattern.")
    }
  }

}
