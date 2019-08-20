/*
 * Copyright 2018-2019 ABSA Group Limited
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

package za.co.absa.atum.persistence

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.atum.utils.ARMImplicits

/** A storer of control measurements to HDFS filesystem as a JSON file . */
class ControlMeasuresStorerJsonFile(hadoopConfiguration: Configuration, path: Path) extends ControlMeasuresStorer {
  override def store(controlInfo: ControlMeasure): Unit = {
    val serialized =  ControlMeasuresParser asJson controlInfo
    saveDataToFile(serialized)
  }

  private def saveDataToFile(data: String): Unit = {
    import ARMImplicits._
    val fs = FileSystem.get(hadoopConfiguration)
    for (fos <- fs.create(
      path,
      new FsPermission("777"),
      true,
      hadoopConfiguration.getInt("io.file.buffer.size", 4096),
      fs.getDefaultReplication(path),
      fs.getDefaultBlockSize(path),
      null)
    ){
      fos.write(data.getBytes)
    }
  }

  override def getInfo: String = {
    s"JSON serializer to ${path.toUri}"
  }
}
