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

package za.co.absa.atum.core

/**
  * The object contains constants and default used through Control Framework
  */
object Constants {
  // Keys to store in Spaek Session
  val InitFlagKey = "control_framework.initialized_flag"
  val InfoFileVersionKey = "control_framework.info_version"
  val InfoFileDateKey = "control_framework.info_date"
  val RunUniqueIdKey = "control_framework.run_unique_id"

  val TimestampFormat = "dd-MM-yyyy HH:mm:ss [Z]"
  val DateFormat = "dd-MM-yyyy"
  val DefaultInfoFileName = "_INFO"

  val controlTypeRecordCount = "controlType.Count"
  val controlTypeDistinctCount = "controlType.distinctCount"
  val controlTypeAggregatedTotal = "controlType.aggregatedTotal"
  val controlTypeAbsAggregatedTotal = "controlType.absAggregatedTotal"
  val controlTypeHashCrc32 = "controlType.HashCrc32"

  val maxErrorMessageSize = 2048
}
