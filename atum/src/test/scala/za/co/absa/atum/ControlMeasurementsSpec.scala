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

package za.co.absa.atum

import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.atum.core.{ControlType, MeasurementProcessor}
import za.co.absa.atum.model.Measurement
import za.co.absa.atum.utils.SparkTestBase

//noinspection ZeroIndexToHead
class ControlMeasurementsSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  import spark.implicits._

  private val schema = StructType(
    Array(
      StructField("id", LongType, nullable = false),
      StructField("price", DecimalType(10, 6)),
      StructField("order", StructType(Array(
        StructField("orderid", LongType),
        StructField("items", IntegerType)))
      )
    ))

  private val measurementsIntOferflow = List(
    Measurement(
      controlType = ControlType.Count.value,
      controlCol = "*",
      controlValue = "2"
    ),
    Measurement(
      controlType = ControlType.AggregatedTotal.value,
      controlCol = "id",
      controlValue = "9223372036854775808"
    ),
    Measurement(
      controlType = ControlType.AbsAggregatedTotal.value,
      controlCol = "id",
      controlValue = "9223372036854775808"
    ),
    Measurement(
      controlType = ControlType.AggregatedTotal.value,
      controlCol = "price",
      controlValue = "2000.2"
    ),
    Measurement(
      controlType = ControlType.AbsAggregatedTotal.value,
      controlCol = "price",
      controlValue = "2000.2"
    ),
    Measurement(
      controlType = ControlType.AggregatedTotal.value,
      controlCol = "order.orderid",
      controlValue = "9223372036854775808"
    ),
    Measurement(
      controlType = ControlType.AbsAggregatedTotal.value,
      controlCol = "order.orderid",
      controlValue = "9223372036854775808"
    ),
    Measurement(
      controlType = ControlType.AggregatedTotal.value,
      controlCol = "order.items",
      controlValue = "2147483648"
    ),
    Measurement(
      controlType = ControlType.AbsAggregatedTotal.value,
      controlCol = "order.items",
      controlValue = "2147483648"
    )
  )

  "integerOverflow" should " be handled by aggregated control measurements" in {
    val inputDataJson = spark.sparkContext.parallelize(
      s"""{"id": ${Long.MaxValue}, "price": 1000.1, "order": { "orderid": 1, "items": 1 } } """ ::
        s"""{"id": 1, "price": 1000.1, "order": { "orderid": ${Long.MaxValue}, "items": ${Int.MaxValue} } } """ :: Nil)
    val df = spark.read
      .schema(schema)
      .json(inputDataJson.toDS)

    val processor = new MeasurementProcessor(measurementsIntOferflow)
    val newMeasurements = processor.measureDataset(df)

    assert(newMeasurements == measurementsIntOferflow)
  }

  private val measurementsAggregation = List(
    Measurement(
      controlType = ControlType.Count.value,
      controlCol = "*",
      controlValue = "2"
    ),
    Measurement(
      controlType = ControlType.AggregatedTotal.value,
      controlCol = "id",
      controlValue = "-1"
    ),
    Measurement(
      controlType = ControlType.AbsAggregatedTotal.value,
      controlCol = "id",
      controlValue = "18446744073709551615"
    ),
    Measurement(
      controlType = ControlType.AggregatedTotal.value,
      controlCol = "price",
      controlValue = "0.1"
    ),
    Measurement(
      controlType = ControlType.AbsAggregatedTotal.value,
      controlCol = "price",
      controlValue = "2000.1"
    ),
    Measurement(
      controlType = ControlType.AggregatedTotal.value,
      controlCol = "order.orderid",
      controlValue = "0"
    ),
    Measurement(
      controlType = ControlType.AbsAggregatedTotal.value,
      controlCol = "order.orderid",
      controlValue = "2"
    ),
    Measurement(
      controlType = ControlType.AggregatedTotal.value,
      controlCol = "order.items",
      controlValue = "0"
    ),
    Measurement(
      controlType = ControlType.AbsAggregatedTotal.value,
      controlCol = "order.items",
      controlValue = "2"
    )
  )

  "aggregationTotal" should "sum all the values, absAggregationTotal should sum absoulte values" in {
    val inputDataJson = spark.sparkContext.parallelize(
      s"""{"id": ${Long.MaxValue}, "price": -1000.0, "order": { "orderid": 1, "items": 1 } } """ ::
        s"""{"id": ${Long.MinValue}, "price": 1000.1, "order": { "orderid": -1, "items": -1 } } """ :: Nil)
    val df = spark.read
      .schema(schema)
      .json(inputDataJson.toDS)

    val processor = new MeasurementProcessor(measurementsAggregation)
    val newMeasurements = processor.measureDataset(df)

    assert(newMeasurements == measurementsAggregation)
  }

  "MeasurementProcessor" should "support numeric data stored in string" in {
    val inputDataJson = spark.sparkContext.parallelize(
      s"""{"id": "100", "price": "-1000.0", "amount": "-10000.100000" } """ ::
        s"""{"id": "-50", "price": "1000.1", "amount": "10000.1000" } """ ::
        s"""{"id": "-50", "price": "0", "amount": "0" } """ :: Nil)

    val schema2 = StructType(
      Array(
        StructField("id", StringType),
        StructField("price", StringType),
        StructField("amount", StringType)
      ))

    val measurements2 = List(
      Measurement(
        controlType = ControlType.AggregatedTotal.value,
        controlCol = "id",
        controlValue = "0"
      ),
      Measurement(
        controlType = ControlType.DistinctCount.value,
        controlCol = "id",
        controlValue = "2"
      ),
      Measurement(
        controlType = ControlType.AbsAggregatedTotal.value,
        controlCol = "price",
        controlValue = "2000.1"
      ),
      Measurement(
        controlType = ControlType.AggregatedTotal.value,
        controlCol = "amount",
        controlValue = "0"
      )
    )

    val df = spark.read
      .schema(schema2)
      .json(inputDataJson.toDS)

    val processor = new MeasurementProcessor(measurements2)
    val newMeasurements = processor.measureDataset(df)

    assert(newMeasurements == measurements2)
  }

  "MeasurementProcessor" should "support handle null result" in {
    val inputDataJson = spark.sparkContext.parallelize(
      s"""{"id": "100", "h1": null, "h2": null, "h3": null, "h4": null, "h5": null, "h6": null  } """ ::
        s"""{"id": "100", "h1": null, "h2": null, "h3": null, "h4": null, "h5": null, "h6": null  } """ :: Nil)

    val schema3 = StructType(
      Array(
        StructField("id", StringType),
        StructField("h1", StringType),
        StructField("h2", StringType),
        StructField("h3", IntegerType),
        StructField("h4", LongType),
        StructField("h5", DoubleType),
        StructField("h6", DecimalType(38, 18))
      ))

    val measurements3 = List(
      Measurement(
        controlType = ControlType.HashCrc32.value,
        controlCol = "h1",
        controlValue = ""
      ),
      Measurement(
        controlType = ControlType.AggregatedTotal.value,
        controlCol = "h1",
        controlValue = "0"
      ),
      Measurement(
        controlType = ControlType.AggregatedTotal.value,
        controlCol = "h2",
        controlValue = "0"
      ),
      Measurement(
        controlType = ControlType.AggregatedTotal.value,
        controlCol = "h3",
        controlValue = "0"
      ),
      Measurement(
        controlType = ControlType.AggregatedTotal.value,
        controlCol = "h4",
        controlValue = "0"
      ),
      Measurement(
        controlType = ControlType.AggregatedTotal.value,
        controlCol = "h5",
        controlValue = "0"
      ),
      Measurement(
        controlType = ControlType.AggregatedTotal.value,
        controlCol = "h6",
        controlValue = "0"
      )
    )

    val df = spark.read
      .schema(schema3)
      .json(inputDataJson.toDS)

    val processor = new MeasurementProcessor(measurements3)
    val newMeasurements = processor.measureDataset(df)

    assert(newMeasurements == measurements3)
  }

  private val measurementsWithHash = List(
    Measurement(
      controlType = ControlType.Count.value,
      controlCol = "*",
      controlValue = "2"
    ),
    Measurement(
      controlType = ControlType.HashCrc32.value,
      controlCol = "id",
      controlValue = "2662510020"
    ),
    Measurement(
      controlType = ControlType.HashCrc32.value,
      controlCol = "name",
      controlValue = "7205431484"
    ),
    Measurement(
      controlType = ControlType.HashCrc32.value,
      controlCol = "price",
      controlValue = "4651009593"
    )
  )

  "controlTypeHash" should "return aggregated hash value for primitive types" in {
    val inputDataJson = spark.sparkContext.parallelize(
      s"""{"id": 1, "name": "Foxes", "price": 100.12 } """ ::
        s"""{"id": 2, "name": "Owls", "price": 200.55 } """ :: Nil)

    val schema = StructType(
      Array(
        StructField("id", LongType, nullable = false),
        StructField("name", StringType),
        StructField("price", DecimalType(10, 6))
      ))

    val df = spark.read
      .schema(schema)
      .json(inputDataJson.toDS)

    val processor = new MeasurementProcessor(measurementsWithHash)
    val newMeasurements = processor.measureDataset(df)

    assert(newMeasurements == measurementsWithHash)
  }

  private val measurementsAggregationShort = List(
    Measurement(
      controlType = "count",
      controlCol = "*",
      controlValue = "2"
    ),
    Measurement(
      controlType = "aggregatedTotal",
      controlCol = "id",
      controlValue = "-1"
    ),
    Measurement(
      controlType = "absAggregatedTotal",
      controlCol = "id",
      controlValue = "18446744073709551615"
    ),
    Measurement(
      controlType = "hashCrc32",
      controlCol = "id",
      controlValue = "3993968105"
    )
  )

  "measurement types" should "be recognized without 'controlType' prefix" in {
    val inputDataJson = spark.sparkContext.parallelize(
      s"""{"id": ${Long.MaxValue}, "price": -1000.0, "order": { "orderid": 1, "items": 1 } } """ ::
        s"""{"id": ${Long.MinValue}, "price": 1000.1, "order": { "orderid": -1, "items": -1 } } """ :: Nil)
    val df = spark.read
      .schema(schema)
      .json(inputDataJson.toDS)

    val processor = new MeasurementProcessor(measurementsAggregationShort)
    val newMeasurements = processor.measureDataset(df)

    assert(newMeasurements == measurementsAggregationShort)
  }

}
