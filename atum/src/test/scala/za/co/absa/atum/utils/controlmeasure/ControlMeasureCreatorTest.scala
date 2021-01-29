package za.co.absa.atum.utils.controlmeasure

import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.atum.model._
import za.co.absa.atum.utils.SparkTestBase
import ControlUtilsSpec._

// testing ControlMeasureCreator + ControlMeasureCreatorBuilder
class ControlMeasureCreatorTest extends AnyFlatSpec with SparkTestBase with Matchers {

  import spark.implicits._

  val colNames = Seq("col1", "col2")
  val testingDf: Dataset[Row] = Seq(
    ("example", 11),
    ("another", 12)
  ).toDF(colNames: _*)

  "ControlMeasureCreator.builder" should "give default creator" in {
    val defaultCreator = ControlMeasureCreator.builder(testingDf, colNames).build
    val expectedCreator = ControlMeasureCreatorImpl(testingDf, colNames)

    defaultCreator shouldBe expectedCreator
  }

  "ControlMeasureCreator" should "give default ControlMeasure" in {
    val defaultCm = ControlMeasureCreator.builder(testingDf, colNames).build.controlMeasure

    val expectedDefaultControlMeasure: ControlMeasure = ControlMeasure(
      ControlMeasureMetadata("", "ZA", "Snapshot", "", "Source", 1, testingDate, Map()),
      None,
      List(
        Checkpoint("Source", Some("Atum"), Some(testingVersion), testingDateTime1, testingDateTime2, "Source", 1, List(
          Measurement("recordCount", "count", "*", "2"),
          Measurement("col1ControlTotal", "hashCrc32", "col1", "4497723351"),
          Measurement("col2ControlTotal", "absAggregatedTotal", "col2", "23")))
      )
    )

    defaultCm.stabilizeTestingControlMeasure shouldBe expectedDefaultControlMeasure
  }

  "ControlMeasureCreator" should "give customized ControlMeasure" in {
    val customCm = ControlMeasureCreator.builder(testingDf, colNames)
      .withSourceApplication("SourceApp1")
      .withInputPath("input/path1")
      .withReportDate("2020-10-20")
      .withCountry("CZ")
      .withHistoryType("HistoryType1")
      .withSourceType("SourceType1")
      .withInitialCheckpointName("InitCheckpoint1")
      .withWorkflowName("Workflow1")
      .build.controlMeasure

    val expectedCustomControlMeasure: ControlMeasure = ControlMeasure(
      ControlMeasureMetadata("SourceApp1", "CZ", "HistoryType1", "input/path1", "SourceType1", 1, testingDate, Map()),
      None,
      List(
        Checkpoint("InitCheckpoint1", Some("Atum"), Some(testingVersion), testingDateTime1, testingDateTime2, "Workflow1", 1, List(
          Measurement("recordCount", "count", "*", "2"),
          Measurement("col1ControlTotal", "hashCrc32", "col1", "4497723351"),
          Measurement("col2ControlTotal", "absAggregatedTotal", "col2", "23")))
      )
    )

    customCm.stabilizeTestingControlMeasure shouldBe expectedCustomControlMeasure
  }

  "ControlMeasureCreator.builder" should "refuse incompatible df+columns" in {
    val message = intercept[IllegalArgumentException] {
      ControlMeasureCreator.builder(testingDf, Seq("nonExistentColName"))
    }.getMessage

    message should include("Aggregated columns must be present in dataset, but 'nonExistentColName' was not found there. Columns found: col1, col2.")
  }

}
