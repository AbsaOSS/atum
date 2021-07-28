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

  val inputData = FileUtils.readFileToString(inputPath)

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
    df.setAdditionalInfo("additionalKey1", "additionalValue1")
    df.setAdditionalInfo("additionalKey1", "anotherAdditionalValue")
    df.setAdditionalInfo("additionalKey2", "additionalValue2")
    df.writeInfoFile(outputPath)

    val updatedData = FileUtils.readFileToString(outputPath)
    val updatedControlMeasure: ControlMeasure = SerializationUtils.fromJson[ControlMeasure](updatedData)

    // assert state of additionalInfo after
    updatedControlMeasure.metadata.additionalInfo should contain(("additionalKey1", "additionalValue1"))
    updatedControlMeasure.metadata.additionalInfo should contain(("additionalKey2", "additionalValue2"))

    fs.delete(new Path(outputPath), false)
    spark.disableControlMeasuresTracking()
  }

  "DataSetWrapper" should "replace existing additionalInfo" in {
    fs.delete(new Path(outputPath), false)

    // Initializing library to hook up to Apache Spark
    spark.enableControlMeasuresTracking(Some(inputPath), None)
      .setControlMeasuresWorkflow("setAdditionalInfo")

    // assert state of additionalInfo before
    inputControlMeasure.metadata.additionalInfo shouldBe Map.empty

    import spark.implicits._
    val df = spark.read.json(Seq(inputData).toDS)

    // act
    df.setAdditionalInfo("additionalKey1", "additionalValue1")
    df.setAdditionalInfo("additionalKey1", "updatedAdditionalValue", true)
    df.writeInfoFile(outputPath)

    val updatedData = FileUtils.readFileToString(outputPath)
    val updatedControlMeasure: ControlMeasure = SerializationUtils.fromJson[ControlMeasure](updatedData)

    // assert state of additionalInfo after
    updatedControlMeasure.metadata.additionalInfo should contain(("additionalKey1", "updatedAdditionalValue"))

    fs.delete(new Path(outputPath), false)
    spark.disableControlMeasuresTracking()
  }
}
