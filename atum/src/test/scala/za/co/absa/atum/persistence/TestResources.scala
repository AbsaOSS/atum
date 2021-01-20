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

import za.co.absa.atum.model.{Checkpoint, ControlMeasure, ControlMeasureMetadata, Measurement}

object TestResources {

  object InputInfo {
    val localPath: String = getClass.getResource("/example_input.info").getPath

    // conforms to the content of the Resource file `example_input.info`
    val controlMeasure = ControlMeasure(
      ControlMeasureMetadata("AtumTest", "CZ", "Snapshot", "example_input.csv", "public", 1, "01-01-2020", Map.empty),
      runUniqueId = None,
      List(Checkpoint("checkpointA", None, None, "01-01-2020 08:00:00", "01-01-2020 08:00:10", "wf1", 1, List(
        Measurement("control1", "someControlType", "column1", "1234")
      )))
    )
  }

  def filterWhitespaces(content: String): String = {
    content.filterNot(_.isWhitespace)
  }

}
