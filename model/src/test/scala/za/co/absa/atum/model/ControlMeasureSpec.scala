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

package za.co.absa.atum.model

import org.scalatest.matchers.should.Matchers

class ControlMeasureSpec extends org.scalatest.flatspec.AnyFlatSpec with Matchers {

  private val cp1 = Checkpoint("prependingCp", None, None, "01-01-2020 07:00:00", "01-01-2020 07:00:10", "wf1", 1, List(
    Measurement("someControlType", "column1", "1234")
  ))

  "ControlMeasure" should "get a new Checkpoint with no checkpoints" in {
    val cm = getTestingControlMeasure(0)
    cm.checkpoints should have length 0

    val updatedCm = cm.withPrecedingCheckpoint(cp1)

    // cp1 prepended as-is
    val expectedCm: ControlMeasure = ControlMeasure(
      ControlMeasureMetadata("AtumTest", "CZ", "Snapshot", "example_input.csv", "public", 1, "01-01-2020", Map.empty),
      runUniqueId = None,
      checkpoints = List(Checkpoint("prependingCp", None, None, "01-01-2020 07:00:00", "01-01-2020 07:00:10", "wf1", 1, List(
        Measurement("someControlType", "column1", "1234"))
    )))

    updatedCm shouldBe expectedCm
  }

  "ControlMeasure" should "get a new Checkpoint with existing checkpoints being shifted back with their order, too" in {
    val cm = getTestingControlMeasure(2)
    cm.checkpoints should have length 2
    cm.checkpoints.map(_.order) shouldBe Seq(1,2)

    val updatedCm = cm.withPrecedingCheckpoint(cp1)

    // cp1 prepended as-is
    updatedCm.checkpoints should have length 3
    updatedCm.checkpoints.head shouldBe cp1
    updatedCm.checkpoints.tail.map(_.order) shouldBe Seq(2,3) // existing order shifted back
  }

  private def getTestingControlMeasure(cpCount: Int): ControlMeasure = {
    require(cpCount >= 0 && cpCount < 10)
    val testingCheckpoints  = Range(0, cpCount).map(_ + 1) // starting with order: 1
      .map { order =>
        Checkpoint(s"orig-cp$order", None, None, s"01-01-2020 0$order:00:00", s"01-01-2020 0$order:00:10", "wf1", order, List(
          Measurement("someControlType", "column1", "1234")
        ))
      }

    ControlMeasure(
      ControlMeasureMetadata("AtumTest", "CZ", "Snapshot", "example_input.csv", "public", 1, "01-01-2020", Map.empty),
      runUniqueId = None,
      checkpoints = testingCheckpoints.toList
    )
  }

}
