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

import org.apache.hadoop.fs.{FileSystem, Path}
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.atum.persistence.{ControlMeasuresParser, HadoopFsControlMeasuresStorer}
import za.co.absa.atum.utils.HdfsFileUtils

/** A storer of control measurements to hadoop filesystem as a JSON file . */
case class ControlMeasuresHdfsStorerJsonFile(path: Path)(implicit val outputFs: FileSystem) extends HadoopFsControlMeasuresStorer {
  override def store(controlInfo: ControlMeasure): Unit = {
    val serialized =  ControlMeasuresParser asJson controlInfo
    HdfsFileUtils.saveStringDataToFile(path, serialized, HdfsFileUtils.getInfoFilePermissions())
  }

  override def getInfo: String = {
    s"JSON serializer to ${path.toUri}"
  }
}
