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

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import za.co.absa.atum.AtumImplicits.{DefaultControlInfoLoader, DefaultControlInfoStorer, StringPathExt}
import za.co.absa.atum.location.S3Location.StringS3LocationExt

object InfoFile {
  /**
   * Sanitizes (removes `?`s and `*`s) and converts string full path to Hadoop FS and Path, e.g.
   * `s3://mybucket1/path/to/file` -> S3 FS + `path/to/file`
   * `/path/on/hdfs/to/file` -> local HDFS + `/path/on/hdfs/to/file`
   *
   * Note, that non-local HDFS paths are not supported in this method, e.g. hdfs://nameservice123:8020/path/on/hdfs/too.
   *
   * @param fullPath path to convert to FS and relative path
   * @param hadoopConfiguration
   * @return FS + relative path
   */
  def convertFullPathToFsAndRelativePath(fullPath: String)(implicit hadoopConfiguration: Configuration): (FileSystem, Path) = {
    val sanitizedFullPath = fullPath.replaceAll("[\\*\\?]", "")

    sanitizedFullPath.toS3Location match {

      case Some(s3Location) =>
        // this is S3 over hadoop FS API, not SDK S3 approach
        val s3Uri = new URI(s3Location.s3String) // s3://<bucket>
        val s3Path = new Path(s"/${s3Location.path}") // /<text-file-object-path>

        val fs = FileSystem.get(s3Uri, hadoopConfiguration)

        (fs, s3Path)

      case None => // local hdfs location
        val fs = FileSystem.get(hadoopConfiguration)

        (fs, sanitizedFullPath.toPath)
    }
  }
}

private[atum] case class InfoFile(infoFilePath: String) {
  require(infoFilePath.nonEmpty, "Empty info file path cannot be used to construct control info stror/loader!")

  def toFsPath(implicit hadoopConfiguration: Configuration): (FileSystem, Path) = {
    InfoFile.convertFullPathToFsAndRelativePath(infoFilePath)
  }

  def toDefaultControlInfoLoader(implicit hadoopConfiguration: Configuration): DefaultControlInfoLoader = {
    val (fs, path) =  toFsPath
    new DefaultControlInfoLoader(path)(fs)
  }

  def toDefaultControlInfoStorer(implicit hadoopConfiguration: Configuration): DefaultControlInfoStorer = {
    val (fs, path) =  toFsPath
    new DefaultControlInfoStorer(path)(fs)
  }

}
