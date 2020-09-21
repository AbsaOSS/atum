package za.co.absa.atum.persistence.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.atum.persistence.TestResources

class ControlMeasuresHdfsLoaderJsonSpec extends AnyFlatSpec with Matchers {

  val inputPath: String = TestResources.InputInfo.localPath
  val expectedInputControlMeasure = TestResources.InputInfo.controlMeasure

  "ControlMeasuresHdfsLoaderJsonFile" should "load json file from HDFS" in {
    val loadedControlMeasure = new ControlMeasuresHdfsLoaderJsonFile(new Configuration(), new Path(inputPath)).load()

    loadedControlMeasure shouldBe expectedInputControlMeasure
  }

}
