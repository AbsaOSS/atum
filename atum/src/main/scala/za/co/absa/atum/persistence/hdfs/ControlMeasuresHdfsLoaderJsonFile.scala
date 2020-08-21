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

package za.co.absa.atum.persistence.hdfs

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.atum.persistence.{ControlMeasuresLoader, ControlMeasuresParser}
import za.co.absa.atum.utils.ControlUtils

import scala.collection.JavaConverters._

/** A loader of control measurements from a JSON file stored in HDFS filesystem. */
class ControlMeasuresHdfsLoaderJsonFile(hadoopConfiguration: Configuration, path: Path) extends ControlMeasuresLoader {
  override def load(): ControlMeasure = {

    val fs = FileSystem.get(hadoopConfiguration)
    val stream = fs.open(path)
    val controlInfoJson = try IOUtils.readLines(stream).asScala.mkString("\n") finally stream.close()

    ControlUtils.preprocessControlMeasure(ControlMeasuresParser fromJson controlInfoJson)
  }
  override def getInfo: String = {
    s"JSON deserializer from ${path.toUri}"
  }
}
