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

package za.co.absa.atum

import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.atum.utils.SparkTestBase
import za.co.absa.atum.utils.controlmeasure.ControlUtils


class ControlUtilsSpec  extends AnyFlatSpec with Matchers with SparkTestBase {
  import spark.implicits._

  private val singleStringColumnDF = spark.sparkContext.parallelize(List("987987", "example", "example", "another example")).toDF
  private val singleStringColumnDF2 = spark.sparkContext.parallelize(List("987987", "another example")).toDF

  private val singleIntColumnDF = spark.sparkContext.parallelize(List(100, 10000, -10000, 999)).toDF
  private val singleIntColumnDF2 = spark.sparkContext.parallelize(List(100, 999)).toDF

  "createInfoFile" should "generates a complete info file" in {
    val testingVersion = "1.2.3"
    val testingDate = "20-02-2020"
    val testingDateTime1 = "20-02-2020 10:20:30 +0100"
    val testingDateTime2 = "20-02-2020 10:20:40 +0100"

    val expected =
      s"""{"metadata":{"sourceApplication":"Test","country":"ZA","historyType":"Snapshot","dataFilename":"/data",
        |"sourceType":"Source","version":1,"informationDate":"$testingDate","additionalInfo":{}},
        |"checkpoints":[{"name":"Source","software":"Atum","version":"$testingVersion","processStartTime":"$testingDateTime1",
        |"processEndTime":"$testingDateTime2","workflowName":"Source","order":1,
        |"controls":[{"controlName":"recordCount","controlType":"count","controlCol":"*","controlValue":"4"},
        |{"controlName":"valueControlTotal","controlType":"absAggregatedTotal","controlCol":"value","controlValue":"21099"}]}]}""".stripMargin.replaceAll("\n", "")

    val actual = ControlUtils.createInfoFile(singleIntColumnDF, "Test", "/data", writeToHDFS = false, prettyJSON = false, aggregateColumns = Seq("value"))
    // replace non-stable fields (date, time, version):
    val actualStabilized = actual
      .replaceFirst("""(?<="informationDate":")(\d{2}-\d{2}-\d{4})""", testingDate)
      .replaceFirst("""(?<="processStartTime":")([-+: \d]+)""", testingDateTime1)
      .replaceFirst("""(?<="processEndTime":")([-+: \d]+)""", testingDateTime2)
      .replaceFirst("""(?<="version":")([-\d\.A-z]+)""", testingVersion)

    assert(actualStabilized == expected)
  }

  "createInfoFile" should "handle integer columns" in {
    val expected = "{\"controlName\":\"valueControlTotal\",\"controlType\":\"absAggregatedTotal\",\"controlCol\":\"value\",\"controlValue\":\"21099\"}]}]}"

    val actual = ControlUtils.createInfoFile(singleIntColumnDF, "Test", "/data", writeToHDFS=false, prettyJSON=false, aggregateColumns = Seq("value"))

    assert(actual.contains(expected))
  }

  "createInfoFile" should "handle numeric values cancellations" in {
    // This test handles cases when wrong aggregator is used for numeric values.
    // For example, if SUM() is used as an aggregator opposite values will cancel each other and
    // the final control value will be the same for datasets containing two values and for a dataset containing none at all
    val matcher = ".*\"controlValue\":\"(\\d+)\".*".r
    val json1 = ControlUtils.createInfoFile(singleIntColumnDF, "Test", "/data", writeToHDFS=false, prettyJSON=false, aggregateColumns = Seq("value"))
    val json2 = ControlUtils.createInfoFile(singleIntColumnDF2, "Test", "/data", writeToHDFS=false, prettyJSON=false, aggregateColumns = Seq("value"))

    val matcher(value1) = json1
    val matcher(value2) = json2

    // control values generated for these 2 datasets should not be the same
    assert (value1 != value2)
  }

  "createInfoFile" should "handle string columns" in {
    val expected = "{\"controlName\":\"valueControlTotal\",\"controlType\":\"hashCrc32\",\"controlCol\":\"value\",\"controlValue\":\"9483370936\"}]}]}"

    val actual = ControlUtils.createInfoFile(singleStringColumnDF, "Test", "/data", writeToHDFS=false, prettyJSON=false, aggregateColumns = Seq("value"))

    assert(actual.contains(expected))
  }

  "createInfoFile" should "handle string hash cancellations" in {
    // This test handles cases when wrong aggregator is used for string hash values.
    // For example, if XOR is used as an aggregator duplicate strings will cancel each other and
    // the final hash will be the same for datasets containing a duplicate values and containing no such values at all

    val matcher = ".*\"controlValue\":\"(\\d+)\".*".r
    val json1 = ControlUtils.createInfoFile(singleStringColumnDF, "Test", "/data", writeToHDFS=false, prettyJSON=false, aggregateColumns = Seq("value"))
    val json2 = ControlUtils.createInfoFile(singleStringColumnDF2, "Test", "/data", writeToHDFS=false, prettyJSON=false, aggregateColumns = Seq("value"))

    val matcher(value1) = json1
    val matcher(value2) = json2

    // control values generated for these 2 datasets should not be the same
    assert (value1 != value2)
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
