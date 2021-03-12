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

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}

/**
  * The class is responsible for listening to Spark events and saving pending Control Framework checkpoint changes.
  */
class SparkEventListener(cf: ControlFrameworkState) extends SparkListener {
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    if (cf.havePendingCheckpoints) {
      Atum.log.info(s"Saving control framework checkpoints")
      cf.updateRunCheckpoints(saveInfoFile = true)
    }
    cf.onApplicationEnd()
  }
}
