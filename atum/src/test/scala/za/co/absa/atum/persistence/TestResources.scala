package za.co.absa.atum.persistence

import za.co.absa.atum.core.ControlType
import za.co.absa.atum.model.{Checkpoint, ControlMeasure, ControlMeasureMetadata, Measurement}

object TestResources {

  object InputInfo {
    val localPath: String = getClass.getResource("/example_input.info").getPath

    // conforms to the content of the Resource file `example_input.info`
    val controlMeasure = ControlMeasure(
      ControlMeasureMetadata("AtumTest", "CZ", "Snapshot", "example_input.csv", "public", 1, "01-01-2020", Map.empty),
      runUniqueId = None,
      List(Checkpoint("checkpointA", None, None, "01-01-2020 08:00:00", "01-01-2020 08:00:10", "wf1", 1, List(
        Measurement("control1", "someControlType", "column1", "1234")
      )))
    )
  }

  def filterWhitespaces(content: String): String = {
    content.filterNot(_.isWhitespace)
  }

}
