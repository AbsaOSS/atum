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

package za.co.absa.atum.examples

import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import za.co.absa.atum.utils.{ARMImplicits, ControlUtils}

/**
  * The object is a Spark Job for creating an info file for a specific data file in a specific format
  *
  * An example command to generate an info file:
  * {{{
  * spark-submit --master yarn \
  *     --deploy-mode client \
  *     --class za.co.absa.atum.examples.CreateInfoFileTool \
  *     atum-examples-0.2.3-SNAPSHOT.jar \
  *     SampleDataSet /user/data/input 3 2017-11-07 parquet employeeId address dealId
  * }}}
  *
  * This routine is only a reference example implementation, it is by no means complete.
  *
  */
object CreateInfoFileTool {

  private val log = LogManager.getLogger("CreateInfoFileJob")

  def main(args: Array[String]) {

    val (sourceApplication: String, inputFileName: String, infoVersion: Int, infoDate: String, rawFormat: String, columnNames: Array[String]) = {
      if (args.length < 5) {
        System.err.println("Usage:\n\tprogram inputFileName datasetVersion date[yyyy-MM-dd] rawFormat [columns for aggregation...]")
        System.exit(1)
      } else {
        (args(0), args(1), args(2).toInt, args(3), args(4), args.slice(5, args.length))
      }
    }

    val dateTokens = infoDate.split("-")
    val dateInDMYFormat = s"${dateTokens(2)}-${dateTokens(1)}-${dateTokens(0)}"

    val sparkBuilder = SparkSession.builder().appName("Create Info File Job")
    val spark = sparkBuilder
      //      .master("local")
      .getOrCreate()

    val ds = spark.read.format(rawFormat).load(inputFileName)

    val strJson = ControlUtils.createInfoFile(ds, sourceApplication, inputFileName, dateInDMYFormat, infoVersion, aggregateColumns = columnNames.toSeq)
  }
}
