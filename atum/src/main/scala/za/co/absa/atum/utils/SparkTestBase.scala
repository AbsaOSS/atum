/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.atum.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait SparkTestBase {
  System.setProperty("user.timezone", "UTC")

  // Do not display INFO entries for tests
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[1]") // Spark3 has async writes. When we move to AsyncFlatSpec, we will change this back to *
    .appName("test")
    .config("spark.sql.codegen.wholeStage", value = false)
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.ui.enabled", "false")
    .config("spark.testing.memory", 1024*1024*1024) // otherwise may fail based on local machine settings
    .getOrCreate()

}
