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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.storage.StorageLevel
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.atum.core.Atum
import za.co.absa.atum.utils.SparkTestBase

class CachingStorageLevelSpec extends AnyFlatSpec with Matchers with SparkTestBase with BeforeAndAfter {

  implicit val fs = FileSystem.get(new Configuration())

  before {
    Atum.init(spark)
  }

  after {
    Atum.dispose(spark)
  }

  "enableCaching" should "enable caching with the default storage level" in {
    Atum.enableCaching()
    assert(Atum.cachingStorageLevel == StorageLevel.MEMORY_AND_DISK)
  }


  "enableCaching" should "enable caching with a specified storage level" in {
    Atum.enableCaching(StorageLevel.MEMORY_ONLY)
    assert(Atum.cachingStorageLevel == StorageLevel.MEMORY_ONLY)
  }

  "enableCaching" should "enable caching with a specified storage level expressed as a string" in {
    Atum.setCachingStorageLevel("MEMORY_ONLY")
    assert(Atum.cachingStorageLevel == StorageLevel.MEMORY_ONLY)
  }

  "disableCaching" should "disable caching" in {
    Atum.disableCaching()
    assert(Atum.cachingStorageLevel == StorageLevel.NONE)
  }

}
