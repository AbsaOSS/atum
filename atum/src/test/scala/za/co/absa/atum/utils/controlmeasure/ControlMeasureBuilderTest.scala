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

package za.co.absa.atum.utils.controlmeasure

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.atum.model._
import za.co.absa.atum.utils.SparkTestBase
import za.co.absa.atum.utils.controlmeasure.ControlMeasureUtilsSpec._

class ControlMeasureBuilderTest extends AnyFlatSpec with SparkTestBase with Matchers {

  import spark.implicits._

  val colNames = Seq("col1", "col2")
  val testingDf: DataFrame = Seq(
    ("example", 11),
    ("another", 12)
  ).toDF(colNames: _*)

  "ControlMeasureBuilder" should "give default ControlMeasure" in {
    val defaultCm = ControlMeasureBuilder.forDF(testingDf).build

    val expectedDefaultControlMeasure: ControlMeasure = ControlMeasure(
      ControlMeasureMetadata("", "ZA", "Snapshot", "", "Source", 1, testingDate, Map()),
      None,
      List(
        Checkpoint("Source", Some(testingSoftware), Some(testingVersion), testingDateTime1, testingDateTime2, "Source", 1, List(
          Measurement("recordCount", "count", "*", "2")
        ))
      )
    )

    defaultCm.stabilizeTestingControlMeasure shouldBe expectedDefaultControlMeasure
  }

  "ControlMeasureBuilder" should "give customized ControlMeasure" in {
    val customCm = ControlMeasureBuilder.forDF(testingDf)
      .withAggregateColumns(colNames)
      .withSourceApplication("SourceApp1")
      .withInputPath("input/path1")
      .withReportDate("2020-10-20")
      .withCountry("CZ")
      .withHistoryType("HistoryType1")
      .withSourceType("SourceType1")
      .withInitialCheckpointName("InitCheckpoint1")
      .withWorkflowName("Workflow1")
      .build

    val expectedCustomControlMeasure: ControlMeasure = ControlMeasure(
      ControlMeasureMetadata("SourceApp1", "CZ", "HistoryType1", "input/path1", "SourceType1", 1, testingDate, Map()),
      None,
      List(
        Checkpoint("InitCheckpoint1", Some(testingSoftware), Some(testingVersion), testingDateTime1, testingDateTime2, "Workflow1", 1, List(
          Measurement("recordCount", "count", "*", "2"),
          Measurement("col1ControlTotal", "hashCrc32", "col1", "4497723351"),
          Measurement("col2ControlTotal", "absAggregatedTotal", "col2", "23")))
      )
    )

    customCm.stabilizeTestingControlMeasure shouldBe expectedCustomControlMeasure
  }

  "ControlMeasureBuilder" should "refuse incompatible df+columns" in {
    val message = intercept[IllegalArgumentException] {
      ControlMeasureBuilder.forDF(testingDf).withAggregateColumns(Seq("nonExistentColName"))
    }.getMessage

    message should include("Aggregate columns must be present in dataframe, but 'nonExistentColName' was not found there. Columns found: col1, col2.")
  }

}
