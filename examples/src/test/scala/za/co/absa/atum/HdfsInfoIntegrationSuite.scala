package za.co.absa.atum

import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.atum.core.Atum
import za.co.absa.atum.model.{Checkpoint, Measurement}
import za.co.absa.atum.persistence.ControlMeasuresParser
import za.co.absa.atum.utils.SparkTestBase

class HdfsInfoIntegrationSuite extends AnyFlatSpec with SparkTestBase with Matchers with BeforeAndAfterAll {

  private val log = LogManager.getLogger(this.getClass)
  val tempDir: String = LocalFsTestUtils.createLocalTemporaryDirectory("hdfsTestOutput")

  override def afterAll = {
    LocalFsTestUtils.safeDeleteTestDir(tempDir)
  }

  private val inputCsv = "data/input/wikidata.csv"
  private def readSparkInputCsv(inputCsvPath: String): DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(inputCsvPath)

  private def writeSparkData(df: DataFrame, outputPath: String): Unit =
    df.write.mode(SaveMode.Overwrite)
      .parquet(outputPath)

  {
    val outputPath = s"$tempDir/outputCheck1"
    // implicit variant only writes to derived outputPath, explicit writes to both implicit derived path and the explicit one, too.
    Seq(
      ("implicit output _INFO path only", "", Seq(s"$outputPath/_INFO")),
      ("implicit & explicit output _INFO path", s"$outputPath/extra/_INFO2", Seq(s"$outputPath/_INFO", s"$outputPath/extra/_INFO2"))
    ).foreach { case (testCaseName, destinationInfoFilePath, expectedPaths) =>

      "_INFO" should s"be written on spark.write ($testCaseName)" in {
        import spark.implicits._
        import za.co.absa.atum.AtumImplicits._

        val hadoopConfiguration = spark.sparkContext.hadoopConfiguration
        implicit val fs = FileSystem.get(hadoopConfiguration)

        implicit val atum = Atum // using basic Atum without extensions

        // Initializing library to hook up to Apache Spark
        spark.enableControlMeasuresTracking(sourceInfoFile = "data/input/wikidata.csv.info", destinationInfoFile = destinationInfoFilePath)
          .setControlMeasuresWorkflow("Job 1")

        val df1 = readSparkInputCsv(inputCsv)
        df1.setCheckpoint("Checkpoint0")
        val filteredDf1 = df1.filter($"total_response_size" > 1000)
        filteredDf1.setCheckpoint("Checkpoint1") // stateful, do not need return value
        writeSparkData(filteredDf1, outputPath) // implicit output _INFO file path is derived from this path passed to spark.write

        spark.disableControlMeasuresTracking()

        expectedPaths.foreach { expectedPath =>
          log.info(s"Checking $expectedPath to contain expected values")

          val infoContentJson = LocalFsTestUtils.readFileAsString(expectedPath)
          val infoControlMeasures = ControlMeasuresParser.fromJson(infoContentJson)

          infoControlMeasures.checkpoints.map(_.name) shouldBe Seq("Source", "Raw", "Checkpoint0", "Checkpoint1")
          val checkpoint0 = infoControlMeasures.checkpoints.collectFirst { case c: Checkpoint if c.name == "Checkpoint0" => c }.get
          checkpoint0.controls should contain(Measurement("recordCount", "count", "*", "5000"))

          val checkpoint1 = infoControlMeasures.checkpoints.collectFirst { case c: Checkpoint if c.name == "Checkpoint1" => c }.get
          checkpoint1.controls should contain(Measurement("recordCount", "count", "*", "4964"))
        }
      }
    }
  }

}
