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

package za.co.absa.atum.persistence

import za.co.absa.atum.model.ControlMeasure
import za.co.absa.atum.utils.ControlUtils

/**
  * This object is used for [[za.co.absa.atum.model.ControlMeasure]] object serialization
  */
object ControlMeasuresParser {
  /**
    * The method returns JSON representation of a [[za.co.absa.atum.model.ControlMeasure]] object
    */
  def asJson(controlMeasure: ControlMeasure): String = {
    ControlUtils.asJson[ControlMeasure](controlMeasure)
  }

  /**
    * The method returns a [[za.co.absa.atum.model.ControlMeasure]] object parsed from JSON string.
    */
  def fromJson(jsonStr: String): ControlMeasure = {
    ControlUtils.fromJson[ControlMeasure](jsonStr)
  }

}
