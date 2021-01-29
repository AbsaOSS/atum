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

package za.co.absa.atum.utils.controlmeasure

import org.slf4j.LoggerFactory

import scala.util.Try

trait ControlMeasureCreatorBuilder {
  def withSourceApplication(sourceApplication: String): ControlMeasureCreatorBuilder
  def withInputPath(inputPath: String): ControlMeasureCreatorBuilder
  def withReportDate(reportDate: String): ControlMeasureCreatorBuilder
  def withReportVersion(reportVersion: Int): ControlMeasureCreatorBuilder
  def withCountry(country: String): ControlMeasureCreatorBuilder
  def withHistoryType(historyType: String): ControlMeasureCreatorBuilder
  def withSourceType(sourceType: String): ControlMeasureCreatorBuilder
  def withInitialCheckpointName(initialCheckpointName: String): ControlMeasureCreatorBuilder
  def withWorkflowName(workflowName: String): ControlMeasureCreatorBuilder

  def build: ControlMeasureCreator
}

/**
 * ControlMeasureCreatorBuilder implementation. Intended to be only created via [[za.co.absa.atum.utils.controlmeasure.ControlMeasureCreator#builder]]
 *
 * @param creator initial ControlMeasureCreatorImpl (has fixed dataset and aggregateColumns).
 */
private case class ControlMeasureCreatorBuilderImpl(private val creator: ControlMeasureCreatorImpl) extends ControlMeasureCreatorBuilder {
  private val logger = LoggerFactory.getLogger(this.getClass)

  // these two are recommended values: failure to fill = warning
  def withSourceApplication(sourceApplication: String): ControlMeasureCreatorBuilderImpl = ControlMeasureCreatorBuilderImpl(creator.copy(sourceApplication = sourceApplication))
  def withInputPath(inputPath: String): ControlMeasureCreatorBuilderImpl = ControlMeasureCreatorBuilderImpl(creator.copy(inputPathName = inputPath))

  def withReportDate(reportDate: String): ControlMeasureCreatorBuilderImpl = {
    if(Try(ControlMeasureUtils.dateFormat.parse(reportDate)).isFailure) {
      logger.error(s"Report date $reportDate does not validate against format ${ControlMeasureUtils.dateFormat}." +
        s"Consider checking correctness of the ControlMeasure")
    }
    ControlMeasureCreatorBuilderImpl(creator.copy(reportDate = reportDate))
  }

  def withReportVersion(reportVersion: Int): ControlMeasureCreatorBuilderImpl = ControlMeasureCreatorBuilderImpl(creator.copy(reportVersion = reportVersion))
  def withCountry(country: String): ControlMeasureCreatorBuilderImpl = ControlMeasureCreatorBuilderImpl(creator.copy(country = country))
  def withHistoryType(historyType: String): ControlMeasureCreatorBuilderImpl = ControlMeasureCreatorBuilderImpl(creator.copy(historyType = historyType))
  def withSourceType(sourceType: String): ControlMeasureCreatorBuilderImpl = ControlMeasureCreatorBuilderImpl(creator.copy(sourceType = sourceType))
  def withInitialCheckpointName(initialCheckpointName: String): ControlMeasureCreatorBuilderImpl = ControlMeasureCreatorBuilderImpl(creator.copy(initialCheckpointName = initialCheckpointName))
  def withWorkflowName(workflowName: String): ControlMeasureCreatorBuilderImpl = ControlMeasureCreatorBuilderImpl(creator.copy(workflowName = workflowName))

  def build: ControlMeasureCreator = {
    if (creator.inputPathName.isEmpty) logger.warn("ControlMeasureCreator's inputPathName is empty!")
    if (creator.sourceApplication.isEmpty) logger.warn("ControlMeasureCreator's sourceApplication is empty!")

    creator
  }
}
