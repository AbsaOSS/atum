package za.co.absa.atum.utils

import org.apache.commons.io.IOUtils
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

}
