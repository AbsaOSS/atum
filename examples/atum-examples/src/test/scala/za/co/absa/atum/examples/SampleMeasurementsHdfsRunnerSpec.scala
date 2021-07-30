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

package za.co.absa.atum.examples

import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.atum.utils._

class SampleMeasurementsHdfsRunnerSpec extends AnyFunSuite
  with SparkJobRunnerMethods
  with SparkLocalMaster {

    // SampleMeasurement2 depends on SampleMeasurements1's output (analogical for SM3), so they should be run in this order
    runSparkJobAsTest[SampleMeasurements1.type]
    runSparkJobAsTest[SampleMeasurements2.type]
    runSparkJobAsTest[SampleMeasurements3.type]
}
