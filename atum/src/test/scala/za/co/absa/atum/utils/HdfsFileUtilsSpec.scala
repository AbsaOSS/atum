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

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.atum.utils.OperatingSystem.OperatingSystems

class HdfsFileUtilsSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  implicit val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  private val Content = "Testing Content"

  "HdfsFileUtils" should "write a file to HDFS (default permissions)" in {
    assume(OperatingSystem.getCurrentOs != OperatingSystems.WINDOWS)
    val path = new Path("/tmp/hdfs-file-utils-test/def-perms.file")

    HdfsFileUtils.getInfoFilePermissionsFromConfig() shouldBe None // key not present, testing default =>
    HdfsFileUtils.saveStringDataToFile(path, Content)

    fs.exists(path) shouldBe true
    fs.getFileStatus(path).getPermission shouldBe HdfsFileUtils.DefaultFilePermissions
    fs.deleteOnExit(path)
  }

  it should "write a file to HDFS (max permissions 777 - default umask 022 -> 755)" in {
    assume(OperatingSystem.getCurrentOs != OperatingSystems.WINDOWS)

    val path = new Path("/tmp/hdfs-file-utils-test/max-perms.file")

    val customConfig = ConfigFactory.empty()
      .withValue("atum.hdfs.info.file.permissions", ConfigValueFactory.fromAnyRef("755"))
    HdfsFileUtils.saveStringDataToFile(path, Content, HdfsFileUtils.getInfoFilePermissionsFromConfig(customConfig).get)

    fs.exists(path) shouldBe true
    // Default 022 umask allows max fsPermissions 755
    fs.getFileStatus(path).getPermission shouldBe new FsPermission("755")
    fs.deleteOnExit(path)
  }

  it should "write a file to HDFS (min permissions)" in {
    assume(OperatingSystem.getCurrentOs != OperatingSystems.WINDOWS)

    val path = new Path("/tmp/hdfs-file-utils-test/min-perms.file")
    val customConfig = ConfigFactory.empty()
      .withValue("atum.hdfs.info.file.permissions", ConfigValueFactory.fromAnyRef("000"))
    HdfsFileUtils.saveStringDataToFile(path, Content, HdfsFileUtils.getInfoFilePermissionsFromConfig(customConfig).get)

    fs.exists(path) shouldBe true
    fs.getFileStatus(path).getPermission shouldBe new FsPermission("000")
    fs.deleteOnExit(path)
  }

  it should "write a file to HDFS (custom permissions)" in {
    assume(OperatingSystem.getCurrentOs != OperatingSystems.WINDOWS)

    val path = new Path("/tmp/hdfs-file-utils-test/custom-perms.file")
    val customConfig = ConfigFactory.empty()
      .withValue("atum.hdfs.info.file.permissions", ConfigValueFactory.fromAnyRef("751"))
    HdfsFileUtils.saveStringDataToFile(path, Content, HdfsFileUtils.getInfoFilePermissionsFromConfig(customConfig).get)

    fs.exists(path) shouldBe true
    fs.getFileStatus(path).getPermission shouldBe new FsPermission("751")
    fs.deleteOnExit(path)
  }

  Seq(
    "garbage$55%$",
    "",
    "1"
  ).foreach { invalidFsPermissionString =>
    it should s"fail on invalid permissions config (case $invalidFsPermissionString)" in {
      val customConfig = ConfigFactory.empty()
        .withValue("atum.hdfs.info.file.permissions", ConfigValueFactory.fromAnyRef(invalidFsPermissionString))

      intercept[IllegalArgumentException] {
        HdfsFileUtils.getInfoFilePermissionsFromConfig(customConfig)
      }
    }
  }


}
