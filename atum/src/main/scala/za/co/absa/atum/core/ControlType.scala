package za.co.absa.atum.core

class ControlType(val value: String)
object ControlType {
  case object Count extends ControlType("count")
  case object DistinctCount extends ControlType("distinctCount")
  case object AggregatedTotal extends ControlType("aggregatedTotal")
  case object AbsAggregatedTotal extends ControlType("absAggregatedTotal")
  case object HashCrc32 extends ControlType("hashCrc32")

  val values = Seq(Count.value, DistinctCount.value, AggregatedTotal.value, AbsAggregatedTotal.value, HashCrc32.value)

  def getNormalizedValue(input: String) = {
    values.find(value => isControlMeasureTypeEqual(input, value)).getOrElse(input)
  }

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
