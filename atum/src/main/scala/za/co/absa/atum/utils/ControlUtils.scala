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

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{abs, col, crc32, sum}
import org.apache.spark.sql.types.{DecimalType, LongType, NumericType}
import org.json4s.{Formats, NoTypeHints, ext}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{write, writePretty}
import za.co.absa.atum.model._
import za.co.absa.atum.core.Constants
import za.co.absa.atum.model._

/**
  * This object contains utilities used in Control Measurements processing
  */
object ControlUtils {
  private val log = LogManager.getLogger("ControlUtils")

  implicit private val formatsJson: Formats = Serialization.formats(NoTypeHints).withBigDecimal + new ext.EnumNameSerializer(RunState)
  private val timestampFormat = DateTimeFormatter.ofPattern(Constants.TimestampFormat)
  private val dateFormat = DateTimeFormatter.ofPattern(Constants.DateFormat)

  /**
    * Get current time as a string formatted according to Control Framework format.
    *
    * @return The current timestamp as a string (e.g. "05-10-2017 09:43:50 +0200")
    */
  def getTimestampAsString: String = {
    val now = ZonedDateTime.now()
    timestampFormat.format(now)
  }

  /**
    * Get current date as a string formatted according to Control Framework format.
    *
    * @return The current date as a string (e.g. "05-10-2017")
    */
  def getTodayAsString: String = {
    val now = ZonedDateTime.now()
    dateFormat.format(now)
  }

  /**
    * The method returns arbitrary object as a Json string.
    *
    * @return A string representing the object in Json format
    */
  def asJson[T <: AnyRef](obj: T): String = {
    write[T](obj)
  }

  /**
    * The method returns arbitrary object as a pretty Json string.
    *
    * @return A string representing the object in Json format
    */
  def asJsonPretty[T <: AnyRef](obj: T): String = {
    writePretty[T](obj)
  }

  /**
    * The method returns arbitrary object parsed from Json string.
    *
    * @return An object deserialized from the Json string
    */
  def fromJson[T <: AnyRef](jsonStr: String )(implicit m: Manifest[T]): T = {
    Serialization.read[T](jsonStr)
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
                     historyType:String = "Snapshot",
                     sourceType: String = "Source",
                     initialCheckpointName: String = "Source",
                     workflowName: String = "Source",
                     writeToHDFS: Boolean = true,
                     prettyJSON: Boolean = true,
                     aggregateColumns: Seq[String]): String = {

    // Calculate the measurements
    val timeStart = getTimestampAsString
    val rowCount = ds.count()
    val aggegatedMeasurements = for (columnName <- aggregateColumns) yield {
      import ds.sparkSession.implicits._

      val dataType = ds.select(columnName).schema.fields(0).dataType

      // This is the aggregated total calculation block
      var controlType = Constants.controlTypeAbsAggregatedTotal
      var controlName = columnName + "Total"
      val aggregatedValue = dataType match {
        case _: LongType =>
          // This is protection against long overflow, e.g. Long.MaxValue = 9223372036854775807:
          //   scala> sc.parallelize(List(Long.MaxValue, 1)).toDF.agg(sum("value")).take(1)(0)(0)
          //   res11: Any = -9223372036854775808
          // Converting to BigDecimal fixes the issue
          val ds2 = ds.select(col(columnName).cast(DecimalType(38,0)).as("value"))
          ds2.agg(sum(abs($"value"))).collect()(0)(0)
        case _: NumericType =>
          ds.agg(sum(abs(col(columnName)))).collect()(0)(0)
        case _ =>
          val aggColName = ControlUtils.getTemporaryColumnName(ds)
          controlType = Constants.controlTypeHashCrc32
          controlName = columnName + "Crc32"
          ds.withColumn(aggColName, crc32(col(columnName).cast("String")))
            .agg(sum(col(aggColName)))
            .collect()(0)(0)
      }

      // Despite aggregated value is Any in Measurement object it should be either a primitive type or Scala's BigDecimal
      // * null values are now empty strings
      // * If the result of the aggregation is java.math.BigDecimal, it is converted to Scala one
      // * If the output is a BigDecimal zero value it is converted to Int(0) so it would not serialize as something like "0+e18"
      val aggregatedValueFixed = aggregatedValue match {
        case null => ""
        case v: java.math.BigDecimal =>
          val valueInScala = scala.math.BigDecimal(v)
          // If it is zero, return zero instead of BigDecimal which can be something like 0E-18
          if (valueInScala == 0) 0 else valueInScala
        case a => a
      }
      Measurement(
        controlName = columnName + "ControlTotal",
        controlType = controlType,
        controlCol = columnName,
        controlValue = aggregatedValueFixed.toString)
    }
    val timeFinish = getTimestampAsString

    // Create a Control Measurement object
    val cm = ControlMeasure(metadata = ControlMeasureMetadata(
      sourceApplication = sourceApplication,
      country = country,
      historyType = historyType,
      dataFilename =  inputPathName,
      sourceType = sourceType,
      version = reportVersion,
      informationDate = reportDate,
      additionalInfo = Map[String, String]()
    ), runUniqueId = None,
      Checkpoint(
        name = initialCheckpointName,
        processStartTime = timeStart,
        processEndTime = timeFinish,
        workflowName = workflowName,
        order = 1,
        controls = Measurement(
          controlName = "recordCount",
          controlType = Constants.controlTypeRecordCount,
          controlCol = "*",
          controlValue = rowCount.toString
        ) :: aggegatedMeasurements.toList
      ) :: Nil )

    val controlMeasuresJson = if (prettyJSON) ControlUtils.asJsonPretty(cm) else ControlUtils.asJson(cm)
    log.info("JSON Generated: " + controlMeasuresJson)

    if (writeToHDFS) {
      val hadoopConfiguration = ds.sparkSession.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(hadoopConfiguration)
      val pathWithoutSpecialChars = inputPathName.replaceAll("[\\*\\?]","")
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
      ){
        fos.write(controlMeasuresJson.getBytes)
      }

      log.warn("Info file written: " + infoPath.toUri.toString)
      log.warn("JSON written: " + controlMeasuresJson)
    }

    // Ensure no exception is thrown on converting back to ControlMeasures object
    ControlUtils.fromJson[ControlMeasure](controlMeasuresJson)
    controlMeasuresJson
  }

}
