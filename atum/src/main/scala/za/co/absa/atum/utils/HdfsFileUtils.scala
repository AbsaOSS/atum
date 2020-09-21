package za.co.absa.atum.utils

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.JavaConverters._

object HdfsFileUtils {

  def readHdfsFileToString(path: Path)(implicit fs: FileSystem): String = {
    val stream = fs.open(path)
    try
      IOUtils.readLines(stream).asScala.mkString("\n")
    finally
      stream.close()
  }

}
