/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package za.co.absa.atum

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.atum.AtumImplicits._
import za.co.absa.atum.core.Atum
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.atum.persistence.TestResources
import za.co.absa.atum.utils.{FileUtils, SerializationUtils, SparkTestBase}

class AtumImplicitsSpec extends AnyFlatSpec with SparkTestBase with Matchers {

  val inputPath: String = TestResources.InputInfo.localPath
  val outputPath = "/tmp/json-setAdditionalInfo-test"

  val inputData: String = FileUtils.readFileToString(inputPath)

  val inputControlMeasure: ControlMeasure = SerializationUtils.fromJson[ControlMeasure](inputData)

  implicit val fs: FileSystem = FileSystem.get(new Configuration())

  "DataSetWrapper" should "set additionalInfo" in {
    fs.delete(new Path(outputPath), false)

    // Initializing library to hook up to Apache Spark
    spark.enableControlMeasuresTracking(Some(inputPath), None)
      .setControlMeasuresWorkflow("setAdditionalInfo")

    // assert state of additionalInfo before
    inputControlMeasure.metadata.additionalInfo shouldBe Map.empty

    import spark.implicits._
    val df = spark.read.json(Seq(inputData).toDS)

    // act
    df.setAdditionalInfo(("additionalKey1", "additionalValue1"))
    df.writeInfoFile(outputPath)

    val updatedData = FileUtils.readFileToString(outputPath)
    val updatedControlMeasure: ControlMeasure = SerializationUtils.fromJson[ControlMeasure](updatedData)

    // assert state of additionalInfo after
    updatedControlMeasure.metadata.additionalInfo should contain(("additionalKey1", "additionalValue1"))

    fs.delete(new Path(outputPath), false)
    spark.disableControlMeasuresTracking()
  }

  "method getControlMeasure" should "return ControlMeasure object" in {
    // Initializing library to hook up to Apache Spark
    spark.enableControlMeasuresTracking(Some(inputPath), None)
      .setControlMeasuresWorkflow("getControlMeasure")

    import spark.implicits._
    val df = spark.read.json(Seq(inputData).toDS)

    // act
    val controlMeasure = df.getControlMeasure

    // assert
    controlMeasure shouldBe inputControlMeasure

    spark.disableControlMeasuresTracking()
  }

  "method getAllAdditionalInfo" should "return Map[String, String] with given info" in {
    // Initializing library to hook up to Apache Spark
    spark.enableControlMeasuresTracking(Some(inputPath), None)
      .setControlMeasuresWorkflow("getControlMeasure")

    import spark.implicits._
    val df = spark.read.json(Seq(inputData).toDS)

    val additionalInfoNoInfo = df.getAllAdditionalInfo
    additionalInfoNoInfo should equal (Map())

    df.setAdditionalInfo(("key1", "value1"))
    df.setAdditionalInfo(("key2", "value2"))
    val expectedAdditionalInfo = Map("key1" -> "value1", "key2" -> "value2")

    val additionalInfo = df.getAllAdditionalInfo
    additionalInfo should equal (expectedAdditionalInfo)

    fs.delete(new Path(outputPath), false)
    spark.disableControlMeasuresTracking()
  }

  "method getAdditionalInfo(key: String)" should "return value for the given key" in {
    // Initializing library to hook up to Apache Spark
    spark.enableControlMeasuresTracking(Some(inputPath), None)
      .setControlMeasuresWorkflow("getControlMeasure")

    import spark.implicits._
    val df = spark.read.json(Seq(inputData).toDS)
    df.setAdditionalInfo(("key1", "value1"))
    df.setAdditionalInfo(("key2", "value2"))

    // act
    val additionalInfoValueExistingKey = df.getAdditionalInfo("key2")
    val additionalInfoValueNonExistingKey = df.getAdditionalInfo("noSuchKey")

    // assert
    additionalInfoValueExistingKey should equal (Some("value2"))
    additionalInfoValueNonExistingKey should equal (None)

    spark.disableControlMeasuresTracking()
  }
}
