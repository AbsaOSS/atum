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

package za.co.absa.atum.utils

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import za.co.absa.atum.AtumImplicits.{DefaultControlInfoLoader, DefaultControlInfoStorer, StringPathExt}
import za.co.absa.atum.location.S3Location.StringS3LocationExt

private[atum] case class InfoFile(infoFile: String) {

  private val validatedInfoFile: Option[String] = if (infoFile.isEmpty) None else Some(infoFile)

  def toOptFsPath(implicit hadoopConfiguration: Configuration): Option[(FileSystem, Path)] = {
    validatedInfoFile.map { definedInfoFile =>
      definedInfoFile.toS3Location match {

        case Some(s3Location) =>
          // this is S3 over hadoop FS API, not SDK S3 approach
          val s3Uri = new URI(s3Location.s3String) // s3://<bucket>
          val s3Path = new Path(s"/${s3Location.path}") // /<text-file-object-path>

          val fs = FileSystem.get(s3Uri, hadoopConfiguration)

          (fs, s3Path)

        case None => // hdfs location
          val fs = FileSystem.get(hadoopConfiguration)

          (fs, definedInfoFile.toPath)
      }
    }
  }

  def toOptDefaultControlInfoLoader(implicit hadoopConfiguration: Configuration): Option[DefaultControlInfoLoader] =
    toOptFsPath.map { case (fs, path) => new DefaultControlInfoLoader(path)(fs)}

  def toOptDefaultControlInfoStorer(implicit hadoopConfiguration: Configuration): Option[DefaultControlInfoStorer] =
    toOptFsPath.map { case (fs, path) => new DefaultControlInfoStorer(path)(fs)}

}
