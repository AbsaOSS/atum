package za.co.absa.atum.utils

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import za.co.absa.atum.AtumImplicits.{DefaultControlInfoLoader, DefaultControlInfoStorer}
import za.co.absa.atum.utils.S3Utils.StringS3LocationExt
import za.co.absa.atum.AtumImplicits.StringPathExt

private[atum] case class InfoFile(infoFile: String)(implicit hadoopConfiguration: Configuration) {

  private val validatedInfoFile: Option[String] = if (infoFile.isEmpty) None else Some(infoFile)

  private def toOptFsPath: Option[(FileSystem, Path)] = {
    validatedInfoFile.map { definedInfoFile =>
      definedInfoFile.toS3Location match {

        case Some(s3Location) =>
          val s3Uri = new URI(s"s3://${s3Location.bucketName}") // s3://<bucket> // what if the user wants to use s3a?
          val s3Path = new Path(s"/${s3Location.path}") // /<text-file-object-path>

          val fs = FileSystem.get(s3Uri, hadoopConfiguration)

          (fs, s3Path)

        case None => // hdfs location
          val fs = FileSystem.get(hadoopConfiguration)

          (fs, definedInfoFile.toPath)
      }
    }
  }

  def toOptDefaultControlInfoLoader: Option[DefaultControlInfoLoader] =
    toOptFsPath.map { case (fs, path) => new DefaultControlInfoLoader(path)(fs)}

  def toOptDefaultControlInfoStorer: Option[DefaultControlInfoStorer] =
    toOptFsPath.map { case (fs, path) => new DefaultControlInfoStorer(path)(fs)}

}
