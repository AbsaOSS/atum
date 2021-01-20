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
      case NonFatal(_) => log.warn(s"Unable to delete a test directory $path")
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
