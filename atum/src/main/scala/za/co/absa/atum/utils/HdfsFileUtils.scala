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

package za.co.absa.atum.utils

import java.io.IOException

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.JavaConverters._

object HdfsFileUtils {

  def readHdfsFileToString(path: Path)(implicit inputFs: FileSystem): String = {
    val stream = inputFs.open(path)
    try
      IOUtils.readLines(stream).asScala.mkString("\n")
    finally
      stream.close()
  }

  /**
   * Writes string data to a HDFS Path
   *
   * @param path Path to write to
   * @param data data to write
   * @param outputFs hadoop FS to use
   * @throws IOException when data write errors occur
   */
  def saveStringDataToFile(path: Path, data: String)(implicit outputFs: FileSystem): Unit = {
    import ARMImplicits._
    for (fos <- outputFs.create(
      path,
      new FsPermission("777"),
      true,
      4096,
      outputFs.getDefaultReplication(path),
      outputFs.getDefaultBlockSize(path),
      null)
         ) {
      fos.write(data.getBytes)
    }
  }

}
