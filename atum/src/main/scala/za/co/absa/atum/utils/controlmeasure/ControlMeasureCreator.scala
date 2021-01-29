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

import org.apache.spark.sql.functions.{abs, col, crc32, sum}
import org.apache.spark.sql.types.{DecimalType, LongType, NumericType}
import org.apache.spark.sql.{Dataset, Row}
import za.co.absa.atum.core.ControlType
import za.co.absa.atum.model.CheckpointImplicits.CheckpointExt
import za.co.absa.atum.model.{Checkpoint, ControlMeasure, ControlMeasureMetadata, Measurement}
import za.co.absa.atum.utils.controlmeasure.ControlMeasureUtils.getTimestampAsString // for Checkpoint.withBuildProperties


trait ControlMeasureCreator {
  def controlMeasure: ControlMeasure
}

object ControlMeasureCreator {

  def builder(ds: Dataset[Row], aggregateColumns: Seq[String]): ControlMeasureCreatorBuilder =
    ControlMeasureCreatorBuilderImpl(ControlMeasureCreatorImpl(ds, aggregateColumns))

}

private case class ControlMeasureCreatorImpl(ds: Dataset[Row],
                                             aggregateColumns: Seq[String],
                                             sourceApplication: String = "",
                                             inputPathName: String = "",
                                             reportDate: String = ControlMeasureUtils.getTodayAsString,
                                             reportVersion: Int = 1,
                                             country: String = "ZA",
                                             historyType: String = "Snapshot",
                                             sourceType: String = "Source",
                                             initialCheckpointName: String = "Source",
                                             workflowName: String = "Source"
                                        ) extends ControlMeasureCreator {

  aggregateColumns.foreach { aggCol =>
    require(ds.columns.contains(aggCol),
      s"Aggregated columns must be present in dataset, but '$aggCol' was not found there. Columns found: ${ds.columns.mkString(", ")}."
    )
  }

  /**
   * Info file control measure - future content of the _INFO file
   *
   * @return
   */
  lazy val controlMeasure: ControlMeasure = {
    // Calculate the measurements
    val timeStart = getTimestampAsString
    val rowCount = ds.count()
    val aggegatedMeasurements = for (columnName <- aggregateColumns) yield {
      import ds.sparkSession.implicits._

      val dataType = ds.select(columnName).schema.fields(0).dataType

      // This is the aggregated total calculation block
      var controlType = ControlType.AbsAggregatedTotal.value
      var controlName = columnName + "Total"
      val aggregatedValue = dataType match {
        case _: LongType =>
          // This is protection against long overflow, e.g. Long.MaxValue = 9223372036854775807:
          //   scala> sc.parallelize(List(Long.MaxValue, 1)).toDF.agg(sum("value")).take(1)(0)(0)
          //   res11: Any = -9223372036854775808
          // Converting to BigDecimal fixes the issue
          val ds2 = ds.select(col(columnName).cast(DecimalType(38, 0)).as("value"))
          ds2.agg(sum(abs($"value"))).collect()(0)(0)
        case _: NumericType =>
          ds.agg(sum(abs(col(columnName)))).collect()(0)(0)
        case _ =>
          val aggColName = ControlMeasureUtils.getTemporaryColumnName(ds)
          controlType = ControlType.HashCrc32.value
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
    ControlMeasure(metadata = ControlMeasureMetadata(
      sourceApplication = sourceApplication,
      country = country,
      historyType = historyType,
      dataFilename = inputPathName,
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
          controlType = ControlType.Count.value,
          controlCol = "*",
          controlValue = rowCount.toString
        ) :: aggegatedMeasurements.toList
      ).withBuildProperties :: Nil)
  }

}
