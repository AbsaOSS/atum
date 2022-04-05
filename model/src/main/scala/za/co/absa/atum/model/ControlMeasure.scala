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

package za.co.absa.atum.model

import za.co.absa.atum.utils.SerializationUtils

case class ControlMeasure
(
  metadata: ControlMeasureMetadata,
  runUniqueId: Option[String],
  checkpoints: List[Checkpoint]
) {
  def asJson: String = SerializationUtils.asJson(this)
  def asJsonPretty: String = SerializationUtils.asJsonPretty(this)

  /**
   * A new ControlMeasure will be constructed with the supplied `checkpoint1` as the new first checkpoint (as-is,
   * e.g. its order value is neither checked nor adjusted).
   * Any existing checkpoints will be shifted behind with their order indices increased by 1.
   *
   * @param checkpoint1 a new checkpoint preceding all the existing
   */
  def withPrecedingCheckpoint(checkpoint1: Checkpoint): ControlMeasure = {
    val shiftedCheckpoints = checkpoints.map { cp =>
      cp.copy(order = cp.order + 1)
    }

    this.copy(checkpoints = checkpoint1 :: shiftedCheckpoints)

  }
}
