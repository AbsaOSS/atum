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

package za.co.absa.atum.core

class ControlType(val value: String, val onlyForNumeric: Boolean)
object ControlType {
  case object Count extends ControlType("count", false)
  case object DistinctCount extends ControlType("distinctCount", false)
  case object AggregatedTotal extends ControlType("aggregatedTotal", true)
  case object AbsAggregatedTotal extends ControlType("absAggregatedTotal", true)
  case object AggregatedTruncTotal extends ControlType("aggregatedTruncTotal", true)
  case object AbsAggregatedTruncTotal extends ControlType("absAggregatedTruncTotal", true)
  case object HashCrc32 extends ControlType("hashCrc32", false)

  val values: Seq[ControlType] = Seq(Count, DistinctCount, AggregatedTotal, AbsAggregatedTotal,
    AggregatedTruncTotal, AbsAggregatedTruncTotal, HashCrc32)
  val valueNames: Seq[String] = values.map(_.value)

  def getNormalizedValueName(input: String): String = {
    valueNames.find(value => isControlMeasureTypeEqual(input, value)).getOrElse(input)
  }

  def withValueName(s: String): ControlType = values.find(_.value.toString == s).getOrElse(
    throw new NoSuchElementException(s"No value found for '$s'. Allowed values are: $valueNames"))

  def isControlMeasureTypeEqual(x: String, y: String): Boolean = {
    if (x.toLowerCase == y.toLowerCase) {
      true
    } else {
      val strippedX = if (x.contains('.')) x.split('.').last.toLowerCase else x.toLowerCase
      val strippedY = if (y.contains('.')) y.split('.').last.toLowerCase else y.toLowerCase
      strippedX == strippedY
    }
  }
}
