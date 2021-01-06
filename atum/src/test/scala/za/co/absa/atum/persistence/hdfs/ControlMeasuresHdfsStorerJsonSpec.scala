package za.co.absa.atum.persistence.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.atum.persistence.TestResources
import za.co.absa.atum.utils.{FileUtils, HdfsFileUtils}

class ControlMeasuresHdfsStorerJsonSpec extends AnyFlatSpec with Matchers {

  val expectedFilePath: String = TestResources.InputInfo.localPath
  val inputControlMeasure: ControlMeasure = TestResources.InputInfo.controlMeasure

  val hadoopConfiguration: Configuration = new Configuration()
  implicit val fs: FileSystem = FileSystem.get(hadoopConfiguration)

  "ControlMeasuresHdfsStorerJsonFile" should "store json file to HDFS" in {

    val outputPath = new Path("/tmp/json-hdfs-storing-test")
    fs.delete(outputPath, false)

    ControlMeasuresHdfsStorerJsonFile(outputPath).store(inputControlMeasure)

    val actualContent = HdfsFileUtils.readHdfsFileToString(outputPath)
    val expectedContent = FileUtils.readFileToString(expectedFilePath)

    // some output may be prettified while other may not, we do not take this into account.
    TestResources.filterWhitespaces(actualContent) shouldBe TestResources.filterWhitespaces(expectedContent)

    fs.delete(outputPath, false)
  }

}
