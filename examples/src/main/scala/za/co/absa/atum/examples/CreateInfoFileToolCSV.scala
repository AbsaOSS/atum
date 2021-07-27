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

package za.co.absa.atum.examples

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.atum.utils.InfoFile
import za.co.absa.atum.utils.controlmeasure.{ControlMeasureBuilder, ControlMeasureUtils}
import za.co.absa.atum.utils.controlmeasure.ControlMeasureUtils.JsonType

/**
  * The object is a Spark Job for creating an info file for a specific CSV file.
  *
  * An example command to generate an info file:
  * {{{
  * spark-submit --master yarn \
  *     --deploy-mode client \
  *     --class za.co.absa.atum.examples.CreateInfoFileToolCSV \
  *     atum-examples-0.2.3-SNAPSHOT.jar \
  *     SampleDataSet /user/data/input 3 2017-11-07 | false ZA Snapshot employeeId address dealId
  * }}}
  *
  * This routine is only a reference example implementation, it is by no means complete.
  *
  */
object CreateInfoFileToolCSV {

  private val log = LogManager.getLogger(this.getClass)

  def main(args: Array[String]) {

    val (sourceApplication: String,
      inputFileName: String,
      infoVersion: Int,
      infoDate: String,
      delimiter: String,
      haveHeaders: Boolean,
      country: String,
      historyType: String,
      columnNames: Array[String]) = {
      if (args.length < 5) {
        System.err.println("Usage:\n\tprogram inputFileName infoVersion infoDate[yyyy-MM-dd] delimiter headers[true/false] " +
          " country historyType [columns for aggregation...]")
        System.exit(1)
      } else {
        (args(0), args(1), args(2).toInt, args(3), args(4), args(5), args(6), args(7), args.slice(8, args.length))
      }
    }

    val dateTokens = infoDate.split("-")
    val dateInDMYFormat = s"${dateTokens(2)}-${dateTokens(1)}-${dateTokens(0)}"

    val sparkBuilder = SparkSession.builder().appName("Create Info File Job")
    val spark = sparkBuilder
      //      .master("local")
      .getOrCreate()

    val ds = spark
      .read
      .format("csv")
      .option("delimiter", delimiter)
      .option("header", haveHeaders)
      .load(inputFileName)

    val cm: ControlMeasure = ControlMeasureBuilder
      .forDF(ds)
      .withAggregateColumns(columnNames.toSeq)
      .withSourceApplication(sourceApplication)
      .withInputPath(inputFileName)
      .withReportDate(dateInDMYFormat)
      .withReportVersion(infoVersion)
      .withCountry(country)
      .withHistoryType(historyType)
      .build

    val hadoopConfiguration = ds.sparkSession.sparkContext.hadoopConfiguration
    val (fs, path) = InfoFile.convertFullPathToFsAndRelativePath(inputFileName)(hadoopConfiguration)

    ControlMeasureUtils.writeControlMeasureInfoFileToHadoopFs(cm, path, JsonType.Pretty)(fs)
  }
}
