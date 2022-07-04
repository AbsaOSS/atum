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

package za.co.absa.atum.core

import za.co.absa.atum.core.Atum.log
import za.co.absa.atum.model
import za.co.absa.atum.persistence.{ControlMeasuresLoader, ControlMeasuresParser, ControlMeasuresStorer}
import za.co.absa.atum.model._
import za.co.absa.atum.model.CheckpointImplicits.CheckpointExt
import za.co.absa.atum.utils.controlmeasure.ControlMeasureUtils

import scala.util.control.NonFatal

/**
  * The class is responsible for accumulating control measurements.
  */
class Accumulator() {
  private var controlMeasure: ControlMeasure = _
  private var storer: ControlMeasuresStorer = _
  private var lastProcessingDate: String = ControlMeasureUtils.getTimestampAsString

  def isControlMeasuresLoaded: Boolean = controlMeasure != null
  def isStorerLoaded: Boolean = storer != null

  /** Loads control measurements */
  def loadControlMeasurements(loaderIn: ControlMeasuresLoader): Unit = {
    try {
      controlMeasure = loaderIn.load()
    } catch {
      case NonFatal(e) =>
        log.warn(s"Unable to load control measurements via ${loaderIn.getInfo}. Error: ${e.getMessage}")
        throw e
    }
  }

  /** Sets a persistor object for saving control checkpoints. */
  def setStorer(storerIn: ControlMeasuresStorer): Unit = {
    storer = storerIn
  }

  /** Gets information about the storer. */
  def getStorerInfo: String = {
    if (storer != null) {
      storer.getInfo
    } else {
      "<no storer>"
    }
  }

  /**
   * Ability to view the storer if set.
   * @return
   */
  private[atum] def getStorer: Option[ControlMeasuresStorer] = if (isStorerLoaded) Some(storer) else None

  /**
    * The method returns Control Info object in which checkpoints are sorted by calculation order.
    */
  def getControlMeasure: ControlMeasure = this.synchronized(
    ControlMeasure(controlMeasure.metadata, controlMeasure.runUniqueId, controlMeasure.checkpoints.sortBy(c => c.order)))

  /** Returns checkpoints that are accumulated up until now. */
  def getCheckpoints: List[Checkpoint] = {
    controlMeasure.checkpoints
  }

  /** Returns the unique ID associated with the sequence of jobs (aka runs) if available. */
  def getUniqueRunId: Option[String] = {
    controlMeasure.runUniqueId
  }

  /** Sets an unique ID to be associated with the sequence of jobs (aka runs). */
  def setRunUniqueId(runUniqueId: String): Unit = {
    controlMeasure = controlMeasure.copy(runUniqueId = Some(runUniqueId))
  }

  /** Adds a key-value pair as an additional information stored in the metadata. */
  def setAdditionalInfo(kv: (String, String), replaceIfExists: Boolean): Unit = {
    controlMeasure.setAdditionalInfo(kv, replaceIfExists)
  }

  /** The method returns Control Info object as a Json string. */
  def asJson: String = {
    ControlMeasuresParser.asJson(getControlMeasure)
  }

  /** Stores control measurements by delegating to the persistence layer. */
  def store(): Unit = {
    if (storer != null) {
      storer.store(getControlMeasure)
    }
  }

  /** The method adds a new checkpoint to the accumulator. */
  def addCheckpoint(name: String,
                    workflowName: String,
                    controls: Seq[Measurement]
                   ): Checkpoint = this.synchronized {
    val order = controlMeasure.checkpoints.map(c => c.order).fold(0)(Math.max) + 1
    val timestampStr = ControlMeasureUtils.getTimestampAsString
    val checkpoint = model.Checkpoint(name = name,
      processStartTime = lastProcessingDate,
      processEndTime = timestampStr,
      workflowName = workflowName,
      order = order,
      controls = controls.toList)
      .withBuildProperties
    lastProcessingDate = timestampStr
    controlMeasure = ControlMeasure(controlMeasure.metadata, controlMeasure.runUniqueId, checkpoint :: controlMeasure.checkpoints)
    checkpoint
  }
}
