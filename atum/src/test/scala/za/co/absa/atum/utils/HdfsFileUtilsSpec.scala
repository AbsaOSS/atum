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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HdfsFileUtilsSpec extends AnyFlatSpec with Matchers with SparkTestBase with MiniDfsClusterBase {

  override def getConfiguration: Configuration = {
    val cfg = new Configuration()
    cfg.set("fs.permissions.umask-mode", "000")
    cfg
  }

  private val Content = "Testing Content"

  "HdfsFileUtils" should "write a file to HDFS (default permissions)" in {
    val path = new Path("/tmp/hdfs-file-utils-test/def-perms.file")
    HdfsFileUtils.saveStringDataToFile(path, Content, HdfsFileUtils.getInfoFilePermissions())

    fs.exists(path) shouldBe true
    fs.getFileStatus(path).getPermission shouldBe HdfsFileUtils.defaultFilePermissions
    fs.deleteOnExit(path)
  }

  it should "write a file to HDFS (max permissions)" in {
    val path = new Path("/tmp/hdfs-file-utils-test/max-perms.file")

    val customConfig = ConfigFactory.empty()
      .withValue("atum.hdfs.info.file.permissions", ConfigValueFactory.fromAnyRef("777"))
    HdfsFileUtils.saveStringDataToFile(path, Content, HdfsFileUtils.getInfoFilePermissions(customConfig))

    fs.exists(path) shouldBe true
    // For this to work, we have miniDfsCluster with umask=000. Default 022 umask would allow max fsPermissions 755
    fs.getFileStatus(path).getPermission shouldBe new FsPermission("777")
    fs.deleteOnExit(path)
  }

  it should "write a file to HDFS (min permissions)" in {
    val path = new Path("/tmp/hdfs-file-utils-test/min-perms.file")
    val customConfig = ConfigFactory.empty()
      .withValue("atum.hdfs.info.file.permissions", ConfigValueFactory.fromAnyRef("000"))
    HdfsFileUtils.saveStringDataToFile(path, Content, HdfsFileUtils.getInfoFilePermissions(customConfig))

    fs.exists(path) shouldBe true
    fs.getFileStatus(path).getPermission shouldBe new FsPermission("000")
    fs.deleteOnExit(path)
  }

  it should "write a file to HDFS (custom permissions)" in {
    val path = new Path("/tmp/hdfs-file-utils-test/custom-perms.file")
    val customConfig = ConfigFactory.empty()
      .withValue("atum.hdfs.info.file.permissions", ConfigValueFactory.fromAnyRef("751"))
    HdfsFileUtils.saveStringDataToFile(path, Content, HdfsFileUtils.getInfoFilePermissions(customConfig))

    fs.exists(path) shouldBe true
    fs.getFileStatus(path).getPermission shouldBe new FsPermission("751")
    fs.deleteOnExit(path)
  }

}
