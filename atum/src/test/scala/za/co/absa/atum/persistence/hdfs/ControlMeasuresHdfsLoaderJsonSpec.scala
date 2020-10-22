package za.co.absa.atum.persistence.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.atum.persistence.TestResources

class ControlMeasuresHdfsLoaderJsonSpec extends AnyFlatSpec with Matchers {

  val inputPath: String = TestResources.InputInfo.localPath
  val expectedInputControlMeasure = TestResources.InputInfo.controlMeasure

  implicit val fs = FileSystem.get(new Configuration())


  "ControlMeasuresHdfsLoaderJsonFile" should "load json file from HDFS" in {
    val loadedControlMeasure = new ControlMeasuresHdfsLoaderJsonFile(new Path(inputPath)).load()

    loadedControlMeasure shouldBe expectedInputControlMeasure
  }

}
