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
