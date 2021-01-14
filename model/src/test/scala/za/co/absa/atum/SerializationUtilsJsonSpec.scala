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

package za.co.absa.atum

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.atum.model.{Checkpoint, ControlMeasure, ControlMeasureMetadata, Measurement}
import za.co.absa.atum.utils.SerializationUtils

/**
  * Unit tests for ControlMeasure SerializationUtils-based object serialization
  */
class SerializationUtilsJsonSpec extends AnyFlatSpec with Matchers {
  val exampleCtrlInfo = ControlMeasure(
    metadata = ControlMeasureMetadata(
      sourceApplication = "FrontArena",
      country = "ZA",
      historyType = "Snapshot",
      dataFilename = "example.dat",
      sourceType = "",
      version = 1,
      informationDate = "01-01-2017",
      additionalInfo = Map("key1" -> "value1", "key2" -> "value2")
    ), None,
    checkpoints = List(Checkpoint(
      name = "Source",
      processStartTime = "01-01-2017 08:00:00",
      processEndTime = "01-01-2017 08:00:00",
      workflowName = "Source",
      order = 1,
      controls = List(
        Measurement(
          controlName = "pvControlTotal",
          controlType = "aggregatedTotal",
          controlCol = "pv",
          controlValue = "32847283324.324324"
        ),
        Measurement(
          controlName = "recordCount",
          controlType = "count",
          controlCol = "id",
          controlValue = "243"
        )),
      software = Some("ExampleSoftware"),
      version = Some("1.2.3")
    ), Checkpoint(
      name = "Raw",
      processStartTime = "01-01-2017 08:00:00",
      processEndTime = "01-01-2017 08:00:00",
      workflowName = "Raw",
      order = 2,
      controls = List(
        Measurement(
          controlName = "pvControlTotal",
          controlType = "aggregatedTotal",
          controlCol = "pv",
          controlValue = "32847283324.324324"
        ),
        Measurement(
          controlName = "recordCount",
          controlType = "count",
          controlCol = "id",
          controlValue = "243"
        )
      ),
      software = Some("AnotherExampleSoftware"),
      version = Some("3.4.5")
    )
    )
  )

  val exampleOutputJson: String = "{\"metadata\":{\"sourceApplication\":\"FrontArena\",\"country\":\"ZA\"," +
    "\"historyType\":\"Snapshot\",\"dataFilename\":\"example.dat\",\"sourceType\":\"\"," +
    "\"version\":1,\"informationDate\":\"01-01-2017\",\"additionalInfo\":{\"key1\":\"value1\",\"key2\":\"value2\"}}," +
    "\"checkpoints\":[{\"name\":\"Source\"," +
    "\"software\":\"ExampleSoftware\",\"version\":\"1.2.3\"," +
    "\"processStartTime\":\"01-01-2017 08:00:00\"," +
    "\"processEndTime\":\"01-01-2017 08:00:00\",\"workflowName\":\"Source\",\"order\":1," +
    "\"controls\":[{\"controlName\":\"pvControlTotal\",\"controlType\":\"aggregatedTotal\"," +
    "\"controlCol\":\"pv\",\"controlValue\":\"32847283324.324324\"},{\"controlName\":\"recordCount\"," +
    "\"controlType\":\"count\",\"controlCol\":\"id\",\"controlValue\":\"243\"}]},{\"name\":\"Raw\"," +
    "\"software\":\"AnotherExampleSoftware\",\"version\":\"3.4.5\"," +
    "\"processStartTime\":\"01-01-2017 08:00:00\",\"processEndTime\":\"01-01-2017 08:00:00\"," +
    "\"workflowName\":\"Raw\",\"order\":2,\"controls\":[{\"controlName\":\"pvControlTotal\"," +
    "\"controlType\":\"aggregatedTotal\",\"controlCol\":\"pv\",\"controlValue\":\"32847283324.324324\"}," +
    "{\"controlName\":\"recordCount\",\"controlType\":\"count\",\"controlCol\":\"id\"," +
    "\"controlValue\":\"243\"}]}]}"

  "asJson" should "serialize a ControlInfo object" in {
    val s = SerializationUtils.asJson(exampleCtrlInfo)
    s shouldEqual exampleOutputJson
  }

  "fromJson" should "deserialize a ControlInfo object" in {
    val s = SerializationUtils.fromJson[ControlMeasure](exampleOutputJson)
    s shouldEqual exampleCtrlInfo
  }

  "asJsonPretty" should "be no worse than asJson" in {
    SerializationUtils.fromJson[ControlMeasure](SerializationUtils.asJsonPretty(exampleCtrlInfo)) shouldEqual exampleCtrlInfo
  }

}
