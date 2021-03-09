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

package za.co.absa.atum.plugins

import za.co.absa.atum.model.{ControlMeasure, RunStatus}

/** Trait for Control Framework events listener. */
trait EventListener {

  /**
    * Called when an _INFO file have been loaded.
    *
    * @param sparkApplicationId An application Id of the Spark job that triggered the event.
    * @param inputInfoFileName The path to an _INFO file that was read.
    * @param controlMeasure The control framework information that was read from the _INFO file.
    */
  def onLoad(sparkApplicationId: String, inputInfoFileName: String, controlMeasure: ControlMeasure): Unit

  /**
    * Called when a checkpoint have been completed.
    *
    * @param controlMeasure The new control framework information containing the new checkpoint.
    */
  def onControlMeasurementsUpdated(controlMeasure: ControlMeasure): Unit

  /**
    * Called when job status changes.
    *
     @param newStatus The new status if the Spark job.
    */
  def onJobStatusChange(newStatus: RunStatus): Unit

  /**
    * Called when a dataset controlled by Control Framework is saved.
    *
    * @param sparkApplicationId An application Id of the Spark job that triggered the event.
    * @param outputPath An path to the file that has been saved.
    */
  def onSaveOutput(sparkApplicationId: String, outputPath: String): Unit

  /**
    * Called when the Spark application ends so the plugin can finalize and release resources.
    */
  def onApplicationEnd(): Unit

}
