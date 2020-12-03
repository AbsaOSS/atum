package za.co.absa.atum

import java.io.File
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.log4j.LogManager

import scala.io.Source
import scala.util.control.NonFatal

object LocalFsTestUtils {
  private val log = LogManager.getLogger(this.getClass)

  /**
   * Creates a temporary directory in the local filesystem.
   *
   * @param prefix A prefix to use for the temporary directory.
   * @return A path to a temporary directory.
   */
  def createLocalTemporaryDirectory(prefix: String): String = {
    val tmpPath = Files.createTempDirectory(prefix)
    tmpPath.toAbsolutePath.toString
  }

  def safeDeleteTestDir(path: String): Unit = {
    try {
      FileUtils.deleteDirectory(new File(path))
    } catch {
      case NonFatal(e) => log.warn(s"Unable to delete a test directory $path")
    }
  }

  def readFileAsString(filename: String, lineSeparator: String = "\n"): String = {
    val sourceFile = Source.fromFile(filename)
    try {
      sourceFile.getLines().mkString(lineSeparator)
    } finally {
      sourceFile.close()
    }
  }

}
