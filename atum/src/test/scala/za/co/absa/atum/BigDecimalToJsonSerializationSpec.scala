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

package za.co.absa.atum

import org.scalatest.{FlatSpec, Matchers}
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import za.co.absa.atum.model.Measurement

class BigDecimalToJsonSerializationSpec extends FlatSpec with Matchers {
  implicit val formats: Formats = Serialization.formats(NoTypeHints).withBigDecimal

  "write" should "serialize a scala.math.BigDecimal" in
  {
    val number = BigDecimal(5.5)
    val s = write(number)
    s shouldEqual "5.5"
  }

  "write" should "serialize a big scala.math.BigDecimal" in
  {
    val number = BigDecimal("32847283324.324324")
    val s = write(number)
    s shouldEqual "32847283324.324324"
  }

  "write" should "serialize a collection with scala.math.BigDecimals" in
  {
    val numbers = Seq(BigDecimal(5.5), BigDecimal(0.56))
    val s = write(numbers)
    s shouldEqual "[5.5,0.56]"
  }

  "write" should "serialize a map with a scala.math.BigDecimal" in
  {
    val map = Map[String, Any]("a" -> "a", "b" -> BigDecimal(5.5))
    val s = write(map)
    s shouldEqual "{\"a\":\"a\",\"b\":5.5}"
  }

  "write" should "serialize a collection of measurements" in
  {
    val mearurements = Seq(
      Measurement(
        controlName = "pvControlTotal",
        controlType = "controlType.aggregatedTotal",
        controlCol = "pv",
        controlValue = "32847283324.324324"
      ),
      Measurement(
        controlName = "recordCount",
        controlType = "controlType.Count",
        controlCol = "id",
        controlValue = "243"
      )
    )
    val s = write(mearurements)
    s shouldEqual "[{\"controlName\":\"pvControlTotal\",\"controlType\":\"controlType.aggregatedTotal\"," + "\"controlCol\":\"pv\",\"controlValue\":\"32847283324.324324\"}," +
                   "{\"controlName\":\"recordCount\",\"controlType\":\"controlType.Count\",\"controlCol\":\"id\",\"controlValue\":\"243\"}]"
  }
}
