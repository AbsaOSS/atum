package za.co.absa.atum

import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.atum.model.{Checkpoint, Measurement}
import za.co.absa.atum.persistence.ControlMeasuresParser
import za.co.absa.atum.utils.SparkTestBase

class HdfsInfoIntegrationSuite extends AnyFlatSpec with SparkTestBase with Matchers  {

  private val log = LogManager.getLogger(this.getClass)


  private val inputCsv = "data/input/wikidata.csv"
  private def readSparkInputCsv(inputCsvPath: String): DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(inputCsvPath)

  private def writeSparkData(df: DataFrame, outputPath: String): Unit =
    df.write.mode(SaveMode.Overwrite)
      .parquet(outputPath)


  "_INFO" should "be written implicitly on spark.write" in {
    val tempDir = LocalFsTestUtils.createLocalTemporaryDirectory("hdfsTestOutput") // todo beforeAll+cleanup afterAll?

    import spark.implicits._
    import za.co.absa.atum.AtumImplicits._

    val hadoopConfiguration = spark.sparkContext.hadoopConfiguration
    implicit val fs = FileSystem.get(hadoopConfiguration)

    // Initializing library to hook up to Apache Spark
    spark.enableControlMeasuresTracking(sourceInfoFile = "data/input/wikidata.csv.info") // todo version with None, None, too?
      .setControlMeasuresWorkflow("Job 1")

    val df1 = readSparkInputCsv(inputCsv)
    df1.setCheckpoint("Checkpoint0")
    val filteredDf1 = df1.filter($"total_response_size" > 1000)
    filteredDf1.setCheckpoint("Checkpoint1") // stateful, do not need return value

    val outputPath = s"$tempDir/hdfsOutput/implicitTest1"
    writeSparkData(filteredDf1, outputPath)

    spark.disableControlMeasuresTracking()

    log.info(s"Checking $outputPath/_INFO to contain expected values")

    val infoContentJson = LocalFsTestUtils.readFileAsString(s"$outputPath/_INFO")
    val infoControlMeasures = ControlMeasuresParser.fromJson(infoContentJson)

    infoControlMeasures.checkpoints.map(_.name) shouldBe Seq("Source", "Raw", "Checkpoint0", "Checkpoint1")
    val checkpoint0 = infoControlMeasures.checkpoints.collectFirst{ case c: Checkpoint if c.name == "Checkpoint0" => c }.get // todo generalize
    checkpoint0.controls should contain (Measurement("recordCount", "count", "*", "5000"))

    val checkpoint1 = infoControlMeasures.checkpoints.collectFirst{ case c: Checkpoint if c.name == "Checkpoint1" => c }.get
    checkpoint1.controls should contain (Measurement("recordCount", "count", "*", "4964"))

    LocalFsTestUtils.safeDeleteTestDir(tempDir)
  }
}
