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

package za.co.absa.atum.utils.controlmeasure

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import za.co.absa.atum.core.{Constants, ControlType}
import za.co.absa.atum.model.{ControlMeasure, Measurement}
import za.co.absa.atum.utils.controlmeasure.ControlMeasureUtils.JsonType.JsonType
import za.co.absa.atum.utils.{HdfsFileUtils, InfoFile, SerializationUtils}

/**
 * This object contains utilities used in Control Measurements processing
 */

object ControlMeasureUtils {
  private val log = LogManager.getLogger("ControlUtils")

  object JsonType extends Enumeration {
    type JsonType = Value
    val Minified, Pretty = Value
  }

  val timestampFormat: DateTimeFormatter = DateTimeFormatter.ofPattern(Constants.TimestampFormat)
  val dateFormat: DateTimeFormatter = DateTimeFormatter.ofPattern(Constants.DateFormat)

  /**
    * Get current time as a string formatted according to Control Framework format [[za.co.absa.atum.utils.controlmeasure.ControlMeasureUtils#timestampFormat]].
    *
    * @return The current timestamp as a string (e.g. "05-10-2017 09:43:50 +0200")
    */
  def getTimestampAsString: String = {
    val now = ZonedDateTime.now()
    timestampFormat.format(now)
  }

  /**
    * Get current date as a string formatted according to Control Framework format [[za.co.absa.atum.utils.controlmeasure.ControlMeasureUtils#dateFormat()]].
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
   * Will write Control Measure `cm` as JSON to Hadoop FS (by default to into the dir specified in `cm.metadata.dataFileName`, file name: _INFO)
   *
   * @param cm        control measure
   * @param outputDir dir on `outputFs`, usual choice is `cm.metadata.dataFileName`
   * @param jsonType  `JsonType.Minified` for compact json (no whitespaces) or `JsonType.Pretty` for indented
   * @param outputFs  hadoop FS. For regular HDFS, use e.g. `FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)` or your S3 FS
   *                  (or rely on e.g. [[za.co.absa.atum.utils.InfoFile#convertFullPathToFsAndRelativePath(java.lang.String, org.apache.hadoop.conf.Configuration]]))
   **/
  def writeControlMeasureInfoFileToHadoopFs(cm: ControlMeasure, outputDir: Path, jsonType: JsonType = JsonType.Minified)(implicit outputFs: FileSystem): Unit = {
    val infoPath = new Path(outputDir, Constants.DefaultInfoFileName)

    val jsonString = jsonType match {
      case JsonType.Minified => cm.asJson
      case JsonType.Pretty => cm.asJsonPretty
    }

    HdfsFileUtils.saveStringDataToFile(infoPath, jsonString)

    log.info("Info file written: " + infoPath.toUri.toString)
    log.info("JSON written: " + jsonString)

    // Ensure no exception is thrown on converting back to ControlMeasures object
    SerializationUtils.fromJson[ControlMeasure](jsonString)
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
