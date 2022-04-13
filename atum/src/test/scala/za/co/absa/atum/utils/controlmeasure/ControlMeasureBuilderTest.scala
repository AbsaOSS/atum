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
import za.co.absa.atum.ControlMeasureBaseTestSuite
import za.co.absa.atum.core.ControlType
import za.co.absa.atum.model._
import za.co.absa.atum.utils.SparkTestBase
import za.co.absa.atum.utils.controlmeasure.ControlMeasureBuilder.ControlTypeStrategy

class ControlMeasureBuilderTest extends AnyFlatSpec with ControlMeasureBaseTestSuite with SparkTestBase with Matchers {

  import spark.implicits._

  val colNames = Seq("col1", "col2", "col3", "col4")
  val testingDf: DataFrame = Seq(
    ("example", 11, "same", -10),
    ("another", 12, "same", -10)
  ).toDF(colNames: _*)


  "ControlMeasureBuilder" should "give default ControlMeasure" in {
    val defaultCm = ControlMeasureBuilder.forDF(testingDf).build

    val expectedDefaultControlMeasure: ControlMeasure = ControlMeasure(
      ControlMeasureMetadata("", "ZA", "Snapshot", "", "Source", 1, testingDate, Map()),
      None,
      List(
        Checkpoint(
          "Source",
          Some(testingSoftware),
          Some(testingVersion),
          testingDateTime1,
          testingDateTime2,
          "Source",
          1,
          List(Measurement("count", "*", "2"))
        )
      )
    )

    // prior to stabilization, let's check the actual by-default generated software fields:
    defaultCm.checkpoints.map(_.software).foreach { swName =>
      swName shouldBe defined
      swName.get should fullyMatch regex ("""^atum_(2\.11|2\.12)$""")
    }

    defaultCm.stabilizeTestingControlMeasure shouldBe expectedDefaultControlMeasure
  }

  it should "give customized ControlMeasure" in {
    val customCm = ControlMeasureBuilder.forDF(testingDf)
      .withAggregateColumns(Seq("col1", "col2"))
      .withSourceApplication("SourceApp1")
      .withInputPath("input/path1")
      .withReportDate("2020-10-20")
      .withCountry("CZ")
      .withHistoryType("HistoryType1")
      .withSourceType("SourceType1")
      .withInitialCheckpointName("InitCheckpoint1")
      .withWorkflowName("Workflow1")
      .withSoftware("MyAwesomeSw", "v1.2.3-beta.4")
      .build

    val expectedCustomControlMeasure: ControlMeasure = ControlMeasure(
      ControlMeasureMetadata("SourceApp1", "CZ", "HistoryType1", "input/path1", "SourceType1", 1, testingDate, Map()),
      None,
      List(
        Checkpoint(
          "InitCheckpoint1",
          Some(testingSoftware),
          Some(testingVersion),
          testingDateTime1,
          testingDateTime2,
          "Workflow1",
          1,
          List(
            Measurement("count", "*", "2"),
            Measurement("hashCrc32", "col1", "4497723351"),
            Measurement("absAggregatedTotal", "col2", "23")
          )
        )
      )
    )

    // prior to stabilization, let's check the custom software/version fields:
    customCm.checkpoints.foreach { cp =>
      cp.software shouldBe Some("MyAwesomeSw")
      cp.version shouldBe Some("v1.2.3-beta.4")
    }

    customCm.stabilizeTestingControlMeasure shouldBe expectedCustomControlMeasure
  }

  import ControlTypeStrategy._
  import ControlType._

  Seq[(String, ControlMeasureBuilder => ControlMeasureBuilder)](
    ("withAggregateColumns implicit default", _.withAggregateColumns(Seq("col1", "col2"))),
    ("withAggregateColumns explicit default", _.withAggregateColumns(Seq("col1", "col2"), Default)),
    ("withAggregateColumn per col - implicit defaults", _.withAggregateColumn("col1").withAggregateColumn("col2")),
    ("withAggregateColumn per col - explicit defaults",
      _.withAggregateColumn("col1", Default).withAggregateColumn("col2", Default)),
    ("withAggregateColumn per col - explicit controlTypes",
      _.withAggregateColumn("col1", Specific(HashCrc32)).withAggregateColumn("col2", AbsAggregatedTotal))
  ).foreach { case (testCaseName, aggColsApplication) =>

    it should s"correctly set default-like aggregateColumns ($testCaseName)" in {
      val customCm = aggColsApplication(ControlMeasureBuilder.forDF(testingDf)).build // one of the .withAggregateColumn(s) above applied

      customCm.checkpoints should have length 1
      customCm.checkpoints.head.controls should contain theSameElementsAs Seq(
        Measurement("count", "*", "2"),
        Measurement("hashCrc32", "col1", "4497723351"),
        Measurement("absAggregatedTotal", "col2", "23")
      )
    }
  }

  it should s"correctly set non-default-like aggregateColumns (count)" in {
    val customCm = ControlMeasureBuilder
      .forDF(testingDf)
      .withAggregateColumn("col1", ControlType.Count)
      .withAggregateColumn("col1", ControlType.DistinctCount) // totals not tested for non-numeric cols
      .withAggregateColumn("col1", ControlType.HashCrc32)

      .withAggregateColumn("col2", ControlType.Count)
      .withAggregateColumn("col2", ControlType.AggregatedTotal)
      .withAggregateColumn("col2", ControlType.AbsAggregatedTotal)
      .withAggregateColumn("col2", ControlType.DistinctCount)
      .withAggregateColumn("col2", ControlType.HashCrc32)

      .withAggregateColumn("col3", ControlType.Count)
      .withAggregateColumn("col3", ControlType.DistinctCount) // totals not tested for non-numeric cols
      .withAggregateColumn("col3", ControlType.HashCrc32)

      .withAggregateColumn("col4", ControlType.Count)
      .withAggregateColumn("col4", ControlType.AggregatedTotal)
      .withAggregateColumn("col4", ControlType.AbsAggregatedTotal)
      .withAggregateColumn("col4", ControlType.DistinctCount)
      .withAggregateColumn("col4", ControlType.HashCrc32)
      .build

    customCm.checkpoints should have length 1
    customCm.checkpoints.head.controls should contain theSameElementsAs Seq(
      Measurement("count", "*", "2"),
      Measurement("count", "col1", "2"),
      Measurement("distinctCount", "col1", "2"),
      Measurement("hashCrc32", "col1", "4497723351"),

      Measurement("count", "col2", "2"),
      Measurement("aggregatedTotal", "col2", "23"),
      Measurement("absAggregatedTotal", "col2", "23"),
      Measurement("distinctCount", "col2", "2"),
      Measurement("hashCrc32", "col2", "4927085124"),

      Measurement("count", "col3", "2"),
      Measurement("distinctCount", "col3", "1"),
      Measurement("hashCrc32", "col3", "8466326152"),

      Measurement("count", "col4", "2"),
      Measurement("aggregatedTotal", "col4", "-20"),
      Measurement("absAggregatedTotal", "col4", "20"),
      Measurement("distinctCount", "col4", "1"),
      Measurement("hashCrc32", "col4", "1587574654")
    )
  }

  it should "refuse incompatible df+columns" in {
    val message = intercept[IllegalArgumentException] {
      ControlMeasureBuilder.forDF(testingDf).withAggregateColumns(Seq("nonExistentColName"))
    }.getMessage

    message should include
      "Aggregate columns must be present in dataframe, but 'nonExistentColName' was not found there. Columns found: col1, col2."
  }

}
