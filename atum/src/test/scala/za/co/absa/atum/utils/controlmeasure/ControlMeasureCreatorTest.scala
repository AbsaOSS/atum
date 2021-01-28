package za.co.absa.atum.utils.controlmeasure

import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.atum.model._
import za.co.absa.atum.utils.SparkTestBase
import za.co.absa.atum.utils.ControlUtilsSpec._

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

  "ControlMeasureCreator" should "give default ControlMeaure" in {
    val defaultCm = ControlMeasureCreator.builder(testingDf, colNames).build.controlMeasure

    val expectedControlMeasure: ControlMeasure = ControlMeasure(
      ControlMeasureMetadata("", "ZA", "Snapshot", "", "Source", 1, testingDate, Map()),
      None,
      List(
        Checkpoint("Source", Some("Atum"), Some(testingVersion), testingDateTime1, testingDateTime2, "Source", 1, List(
          Measurement("recordCount", "count", "*", "2"),
          Measurement("col1ControlTotal", "hashCrc32", "col1", "4497723351"),
          Measurement("col2ControlTotal", "absAggregatedTotal", "col2", "23")))
      )
    )

    defaultCm.stabilizeControlMeasure shouldBe expectedControlMeasure
  }

  "ControlMeasureCreator.builder" should "refuse incompatible df+columns" in {
    val message = intercept[IllegalArgumentException] {
      ControlMeasureCreator.builder(testingDf, Seq("nonExistentColName"))
    }.getMessage

    message should include("Aggregated columns must be present in dataset, but 'nonExistentColName' was not found there. Columns found: col1, col2.")
  }

}
