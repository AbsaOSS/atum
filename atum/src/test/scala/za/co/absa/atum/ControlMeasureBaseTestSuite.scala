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

import za.co.absa.atum.model.{Checkpoint, ControlMeasure}

trait ControlMeasureBaseTestSuite {
  val testingVersion = "1.2.3"
  val testingSoftware = "Atum"
  val testingDate = "20-02-2020"
  val testingDateTime1 = "20-02-2020 10:20:30 +0100"
  val testingDateTime2 = "20-02-2020 10:20:40 +0100"

  /**
   * Replaces metadata.informationDate, checkpoints.[].{version, processStartTime, processEndTime} with ControlUtilsSpec.testing* values
   * (and replaces CRLF endings with LF if found, too) in JSON
   *
   * @param actualJson
   * @return updated json
   */
  def stabilizeJsonOutput(actualJson: String): String = {
    actualJson
      .replaceFirst("""(?<="informationDate"\s?:\s?")(\d{2}-\d{2}-\d{4})""", testingDate)
      .replaceAll("""(?<="processStartTime"\s?:\s?")([-+: \d]+)""", testingDateTime1)
      .replaceAll("""(?<="processEndTime"\s?:\s?")([-+: \d]+)""", testingDateTime2)
      .replaceAll("""(?<="version"\s?:\s?")([-\d\.A-z]+)""", testingVersion)
      .replaceAll("""(?<="software"\s?:\s?")([\d\.A-z_]+)""", testingSoftware)
      .replaceAll("\r\n", "\n") // Windows guard
  }

  implicit class ControlMeasureStabilizationExt(cm: ControlMeasure) {
    def replaceInformationDate(newDate: String): ControlMeasure = cm.copy(metadata = cm.metadata.copy(informationDate = newDate))

    def updateCheckpoints(fn: Checkpoint => Checkpoint): ControlMeasure = cm.copy(checkpoints = cm.checkpoints.map(fn))

    def replaceCheckpointsVersion(newVersion: Option[String]): ControlMeasure = cm.updateCheckpoints(_.copy(version = newVersion))
    def replaceCheckpointsSoftware(newSoftware: Option[String]): ControlMeasure = cm.updateCheckpoints(_.copy(software = newSoftware))
    def replaceCheckpointsProcessStartTime(newDateTime: String): ControlMeasure = cm.updateCheckpoints(_.copy(processStartTime = newDateTime))
    def replaceCheckpointsProcessEndTime(newDateTime: String): ControlMeasure = cm.updateCheckpoints(_.copy(processEndTime = newDateTime))

    def stabilizeTestingControlMeasure: ControlMeasure = {
      cm.replaceInformationDate(testingDate)
        .replaceCheckpointsVersion(Some(testingVersion))
        .replaceCheckpointsSoftware(Some(testingSoftware))
        .replaceCheckpointsProcessStartTime(testingDateTime1)
        .replaceCheckpointsProcessEndTime(testingDateTime2)
    }
  }
}
