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

package za.co.absa.atum.utils

import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{write, writePretty}
import org.json4s.{Formats, NoTypeHints, ext}
import za.co.absa.atum.model._

/**
  * This object contains utilities used in Control Measurements processing
  */
object SerializationUtils {

  implicit private val formatsJson: Formats = Serialization.formats(NoTypeHints).withBigDecimal + new ext.EnumNameSerializer(RunState)

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
  def fromJson[T <: AnyRef](jsonStr: String)(implicit m: Manifest[T]): T = {
    Serialization.read[T](jsonStr)
  }

}
