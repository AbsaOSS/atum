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
import za.co.absa.atum.persistence.TestResources
import za.co.absa.atum.utils.SparkLocalMaster

class ControlMeasuresHdfsLoaderJsonSpec extends AnyFlatSpec with Matchers with SparkLocalMaster {

  val inputPath: String = TestResources.InputInfo.localPath
  val expectedInputControlMeasure = TestResources.InputInfo.controlMeasure

  implicit val fs = FileSystem.get(new Configuration())


  "ControlMeasuresHdfsLoaderJsonFile" should "load json file from HDFS" in {
    val loadedControlMeasure = ControlMeasuresHdfsLoaderJsonFile(new Path(inputPath)).load()

    loadedControlMeasure shouldBe expectedInputControlMeasure
  }

}
