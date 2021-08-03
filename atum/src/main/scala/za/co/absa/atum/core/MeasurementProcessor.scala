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

package za.co.absa.atum.core

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, LongType, StringType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import za.co.absa.atum.core.ControlType._
import za.co.absa.atum.core.MeasurementProcessor.MeasurementFunction
import za.co.absa.atum.model.Measurement
import za.co.absa.atum.utils.controlmeasure.ControlMeasureUtils

import scala.util.{Failure, Success, Try}

object MeasurementProcessor {
  type MeasurementFunction = DataFrame => String

  private val valueColumnName: String = "value"

  /**
   * Assembles the measurement function based for controlCol based on the controlType
   * @param controlCol
   * @param controlType
   * @return
   */
  private[atum] def getMeasurementFunction(controlCol: String, controlType: ControlType): MeasurementFunction = {
    controlType match {
      case Count => (ds: Dataset[Row]) => ds.count().toString
      case DistinctCount => (ds: Dataset[Row]) => {
        ds.select(col(controlCol)).distinct().count().toString
      }
      case AggregatedTotal =>
        (ds: Dataset[Row]) => {
          val aggCol = sum(col(valueColumnName))
          aggregateColumn(ds, controlCol, aggCol)
        }
      case AbsAggregatedTotal =>
        (ds: Dataset[Row]) => {
          val aggCol = sum(abs(col(valueColumnName)))
          aggregateColumn(ds, controlCol, aggCol)
        }
      case HashCrc32 =>
        (ds: Dataset[Row]) => {
          val aggColName = ControlMeasureUtils.getTemporaryColumnName(ds)
          val v = ds.withColumn(aggColName, crc32(col(controlCol).cast("String")))
            .agg(sum(col(aggColName))).collect()(0)(0)
          if (v == null) "" else v.toString
        }
    }
  }

  private def aggregateColumn(ds: Dataset[Row], measureColumn: String, aggExpression: Column): String = {
    val dataType = ds.select(measureColumn).schema.fields(0).dataType
    val aggregatedValue = dataType match {
      case _: LongType =>
        // This is protection against long overflow, e.g. Long.MaxValue = 9223372036854775807:
        //   scala> sc.parallelize(List(Long.MaxValue, 1)).toDF.agg(sum("value")).take(1)(0)(0)
        //   res11: Any = -9223372036854775808
        // Converting to BigDecimal fixes the issue
        //val ds2 = ds.select(col(measurement.controlCol).cast(DecimalType(38, 0)).as("value"))
        //ds2.agg(sum(abs($"value"))).collect()(0)(0)
        val ds2 = ds.select(col(measureColumn).cast(DecimalType(38, 0)).as(valueColumnName))
        val collected = ds2.agg(aggExpression).collect()(0)(0)
        if (collected == null) 0 else collected
      case _: StringType =>
        // Support for string type aggregation
        val ds2 = ds.select(col(measureColumn).cast(DecimalType(38, 18)).as(valueColumnName))
        val collected = ds2.agg(aggExpression).collect()(0)(0)
        val value = if (collected==null) new java.math.BigDecimal(0) else collected.asInstanceOf[java.math.BigDecimal]
        value
          .stripTrailingZeros    // removes trailing zeros (2001.500000 -> 2001.5, but can introduce scientific notation (600.000 -> 6E+2)
          .toPlainString         // converts to normal string (6E+2 -> "600")
      case _ =>
        val ds2 = ds.select(col(measureColumn).as(valueColumnName))
        val collected = ds2.agg(aggExpression).collect()(0)(0)
        if (collected == null) 0 else collected
    }
    //check if total is required to be presented as larger type - big decimal
    workaroundBigDecimalIssues(aggregatedValue)
  }

  private def workaroundBigDecimalIssues(value: Any): String = {
    // If aggregated value is java.math.BigDecimal, convert it to scala.math.BigDecimal
    value match {
      case v: java.math.BigDecimal =>
        // Convert the value to string to workaround different serializers generate different JSONs for BigDecimal
        v.stripTrailingZeros    // removes trailing zeros (2001.500000 -> 2001.5, but can introduce scientific notation (600.000 -> 6E+2)
          .toPlainString         // converts to normal string (6E+2 -> "600")
      case v: BigDecimal =>
        // Convert the value to string to workaround different serializers generate different JSONs for BigDecimal
        new java.math.BigDecimal(v.toString())
          .stripTrailingZeros    // removes trailing zeros (2001.500000 -> 2001.5, but can introduce scientific notation (600.000 -> 6E+2)
          .toPlainString         // converts to normal string (6E+2 -> "600")
      case a => a.toString
    }
  }

}

/**
  * This class is used for processing Spark Dataset to calculate aggregates / control measures
  */
class MeasurementProcessor(private var measurements: Seq[Measurement]) {
  type MeasurementProcessor = (Measurement, MeasurementFunction)

  // Assigning measurement function to each measurement
  var processors: Seq[MeasurementProcessor] =
    measurements.map(m => (m, getMeasurementFunction(m)))


  /** The method calculates measurements for each control.  */
  private[atum] def measureDataset(ds: Dataset[Row]): Seq[Measurement] = {
    Atum.log.debug(s"Schema: ${ds.schema.treeString}")
    processors.map(p => Measurement(controlName = p._1.controlName,
      controlType = p._1.controlType,
      controlCol = p._1.controlCol,
      controlValue = p._2(ds) // call measurement function
    ))
  }

  /** Register new column name so that the new name is used when calculating a new checkpoint.  */
  private[atum] def registerColumnRename(oldName: String, newName: String): Unit = {
    val oldLowercaseName = oldName.trim.toLowerCase
    val newMeasurements = measurements.map(measure => {
      if (measure.controlCol.trim.toLowerCase == oldLowercaseName) {
        measure.copy(controlCol = newName)
      }
      else {
        measure
      }
    })

    processors = newMeasurements.map(m => (m, getMeasurementFunction(m)))
    measurements = newMeasurements
  }

  /** Register a column drop so no measurements tracking is necessary.  */
  private[atum] def registerColumnDrop(columnName: String): Unit = {
    val oldLowercaseName = columnName.trim.toLowerCase
    val newMeasurements = measurements.filter(measure => measure.controlCol.trim.toLowerCase != oldLowercaseName)

    processors = newMeasurements.map(m => (m, getMeasurementFunction(m)))
    measurements = newMeasurements
  }

  /** The method maps string representation of control type to measurement function.  */
  private def getMeasurementFunction(measurement: Measurement): MeasurementFunction = {
    Try {
      ControlType.withValueName(measurement.controlType)
    } match {
      case Failure(exception) =>
        Atum.log.error(s"Unrecognized control measurement type '${measurement.controlType}'. Available control measurement types are: " +
          s"${ControlType.values.mkString(",")}.")
        Atum.log.error(exception.getLocalizedMessage)
        (_: Dataset[Row]) => "N/A"

      case Success(controlType) =>
        MeasurementProcessor.getMeasurementFunction(measurement.controlCol, controlType)
    }
  }

}
