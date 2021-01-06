package za.co.absa.atum.utils

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import za.co.absa.atum.AtumImplicits.{DefaultControlInfoLoader, DefaultControlInfoStorer}
import za.co.absa.atum.utils.S3Utils.StringS3LocationExt
import za.co.absa.atum.AtumImplicits.StringPathExt

private[atum] case class InfoFile(infoFile: String) {

  private val validatedInfoFile: Option[String] = if (infoFile.isEmpty) None else Some(infoFile)

  def toOptFsPath(implicit hadoopConfiguration: Configuration): Option[(FileSystem, Path)] = {
    validatedInfoFile.map { definedInfoFile =>
      definedInfoFile.toS3Location match {

        case Some(s3Location) =>
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
