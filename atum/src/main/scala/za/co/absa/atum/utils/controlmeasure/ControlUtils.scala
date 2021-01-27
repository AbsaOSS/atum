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

package za.co.absa.atum.utils.controlmeasure

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import za.co.absa.atum.core.{Constants, ControlType}
import za.co.absa.atum.model.{ControlMeasure, Measurement}
import za.co.absa.atum.utils.{ARMImplicits, SerializationUtils}

/**
 * This object contains utilities used in Control Measurements processing
 */

object ControlUtils {
  private val log = LogManager.getLogger("ControlUtils")

  val timestampFormat: DateTimeFormatter = DateTimeFormatter.ofPattern(Constants.TimestampFormat)
  val dateFormat: DateTimeFormatter = DateTimeFormatter.ofPattern(Constants.DateFormat)

  /**
    * Get current time as a string formatted according to Control Framework format [[za.co.absa.atum.utils.controlmeasure.ControlUtils#timestampFormat]].
    *
    * @return The current timestamp as a string (e.g. "05-10-2017 09:43:50 +0200")
    */
  def getTimestampAsString: String = {
    val now = ZonedDateTime.now()
    timestampFormat.format(now)
  }

  /**
    * Get current date as a string formatted according to Control Framework format [[za.co.absa.atum.utils.controlmeasure.ControlUtils#dateFormat()]].
    *
    * @return The current date as a string (e.g. "05-10-2017")
    */
  def getTodayAsString: String = {
    val now = ZonedDateTime.now()
    dateFormat.format(now)
  }

  /**
    * The method generates a temporary column name which does not exist in the specified `DataFrame`.
    *
    * @return An column name as a string
    */
  def getTemporaryColumnName(df: DataFrame, namePrefix: String = "tmp"): String = {
    val r = scala.util.Random
    var tempColumnName = ""
    do {
      tempColumnName = s"${namePrefix}_${r.nextInt(10000).toString}"
    } while (df.schema.fields.exists(field => field.name.compareToIgnoreCase(tempColumnName) == 0))
    tempColumnName
  }

  /**
    * The method crates an _INFO file for a given dataset.
    * The row count measurement is added automatically. You can also specify aggregation columns for
    * aggregation measurements
    *
    * @param ds A dataset for which _INFO file to be created.
    * @param sourceApplication The name of the application providing the data.
    * @param inputPathName The path to the input file name. Can be a folder with file mask.
    * @param reportDate The date of the data generation (default = today).
    * @param reportVersion The version of the data generation for the date, new versions replace old  versions of data (default = 1).
    * @param country Country name (default = "ZA").
    * @param historyType History type (default = "Snapshot").
    * @param sourceType Source type (default = "Source").
    * @param initialCheckpointName The name of the initial checkpoint (default = "Source").
    * @param workflowName A workflow name to group several checkpoint sth in the chain (default = "Source").
    * @param writeToHDFS A flag specifying if saving _INFO file to HDFS needed. If false the _INFO file will not be saved to HDFS.
    * @param prettyJSON Output pretty JSON.
    * @param aggregateColumns Numeric column names for.
    *
    * @return The content of the _INFO file.
    */
  def createInfoFile(ds: Dataset[Row],
                     sourceApplication: String,
                     inputPathName: String,
                     reportDate: String = getTodayAsString,
                     reportVersion: Int = 1,
                     country: String = "ZA",
                     historyType: String = "Snapshot",
                     sourceType: String = "Source",
                     initialCheckpointName: String = "Source",
                     workflowName: String = "Source",
                     writeToHDFS: Boolean = true,
                     prettyJSON: Boolean = true,
                     aggregateColumns: Seq[String]): String = {

    // Calculate the measurements
    val cm: ControlMeasure = ControlMeasureCreator
      .builder(ds, aggregateColumns)
      .withSourceApplication(sourceApplication)
      .withInputPath(inputPathName)
      .withReportDate(reportDate)
      .withReportVersion(reportVersion)
      .withCountry(country)
      .withHistoryType(historyType)
      .withSourceType(sourceType)
      .withInitialCheckpointName(initialCheckpointName)
      .withWorkflowName(workflowName)
      .build
      .controlMeasure

    val controlMeasuresJson = if (prettyJSON) cm.asJsonPretty else cm.asJson
    log.info("JSON Generated: " + controlMeasuresJson)

    if (writeToHDFS) {
      val hadoopConfiguration = ds.sparkSession.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(hadoopConfiguration)
      val pathWithoutSpecialChars = inputPathName.replaceAll("[\\*\\?]", "")
      val infoPath = new Path(pathWithoutSpecialChars, Constants.DefaultInfoFileName)

      import ARMImplicits._
      for (fos <- fs.create(
        infoPath,
        new FsPermission("777"),
        true,
        hadoopConfiguration.getInt("io.file.buffer.size", 4096),
        fs.getDefaultReplication(infoPath),
        fs.getDefaultBlockSize(infoPath),
        null)
           ) {
        fos.write(controlMeasuresJson.getBytes)
      }

      log.warn("Info file written: " + infoPath.toUri.toString)
      log.warn("JSON written: " + controlMeasuresJson)
    }

    // Ensure no exception is thrown on converting back to ControlMeasures object
    SerializationUtils.fromJson[ControlMeasure](controlMeasuresJson)
    controlMeasuresJson
  }

  def preprocessControlMeasure: ControlMeasure => ControlMeasure = convertControlValuesToStrings _ andThen normalize

  /**
    * Converts all measurements in an instance of [[ControlMeasure]] object into stings so it won't cause
    * confusion when deserialized downstream.
    *
    * @param controlMeasure A control measures.
    *
    * @return The converted control measurements.
    */
  def convertControlValuesToStrings(controlMeasure: ControlMeasure): ControlMeasure = {
    transformMeasurementInControlMeasure(controlMeasure, measurement => {
      measurement.copy(controlValue = measurement.controlValue.toString)
    })
  }

  /**
   * Normalizes all measurements in an instance of [[ControlMeasure]] object into standard values
   *
   * @param controlMeasure A control measures.
   *
   * @return The normalized control measurements.
   */
  def normalize(controlMeasure: ControlMeasure): ControlMeasure = {
    transformMeasurementInControlMeasure(controlMeasure, measurement => {
      measurement.copy(controlType = ControlType.getNormalizedValue(measurement.controlType))
    })
  }

  private def transformMeasurementInControlMeasure(controlMeasure: ControlMeasure, transformation: Measurement => Measurement) = {
    val newCheckpoints = controlMeasure.checkpoints.map(checkpoint => {
      val newControls = checkpoint.controls.map(transformation)
      checkpoint.copy(controls = newControls)
    })
    controlMeasure.copy(checkpoints = newCheckpoints)
  }
}
