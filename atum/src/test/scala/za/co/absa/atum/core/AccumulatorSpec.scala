package za.co.absa.atum.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import za.co.absa.atum.model.{ControlMeasure, ControlMeasureMetadata}
import za.co.absa.atum.persistence.ControlMeasuresLoader

class AccumulatorSpec extends AnyFlatSpec {

  private val emptyControlMeasureMetadata = ControlMeasureMetadata(
    sourceApplication = "",
    country = "",
    historyType = "",
    dataFilename = "",
    sourceType = "",
    version = 0,
    informationDate = "",
    additionalInfo = Map.empty
  )
  private val emptyControlMeasure: ControlMeasure = ControlMeasure(
    emptyControlMeasureMetadata,
    None,
    List.empty
  )
  private def initAccumulator(controlMeasure: ControlMeasure = emptyControlMeasure): Accumulator = {
    val result = new Accumulator
    val loader: ControlMeasuresLoader = new ControlMeasuresLoader{
      override def load(): ControlMeasure = controlMeasure
      override def getInfo: String = ""
    }
    result.loadControlMeasurements(loader)
    result
  }

  private def initAccumulator(controlMeasureMetadata: ControlMeasureMetadata): Accumulator = {
    initAccumulator(emptyControlMeasure.copy(metadata = controlMeasureMetadata))
  }

  "setAdditionalInfo" should "add additional key" in {
    val expected = emptyControlMeasureMetadata.copy(additionalInfo = Map("Luke"->"Skywalker", "Han"->"Solo"))

    val accumulator = initAccumulator()
    accumulator.setAdditionalInfo(("Luke","Skywalker"), replaceIfExists = false)
    accumulator.setAdditionalInfo(("Han","Solo"), replaceIfExists = true)
    val actual = accumulator.getControlMeasure.metadata
    actual shouldBe expected
  }

  it should "overwrite a key with overwrite on" in {
    val initControlMeasureMetadata = emptyControlMeasureMetadata.copy(additionalInfo = Map("Leia"->"Organa", "Han"->"Solo"))
    val expected = emptyControlMeasureMetadata.copy(additionalInfo = Map("Leia"->"Organa Solo", "Han"->"Solo"))
    val accumulator = initAccumulator(initControlMeasureMetadata)
    accumulator.setAdditionalInfo(("Leia","Organa Solo"), replaceIfExists = true)
    val actual = accumulator.getControlMeasure.metadata
    actual shouldBe expected
  }

  it should "keep the old value if overwrite is off" in {
    val initControlMeasureMetadata = emptyControlMeasureMetadata.copy(additionalInfo = Map("Luke"->"Skywalker", "Han"->"Solo"))
    val accumulator = initAccumulator(initControlMeasureMetadata)
    accumulator.setAdditionalInfo(("Luke","Vader"), replaceIfExists = false)
    val actual = accumulator.getControlMeasure.metadata
    actual shouldBe initControlMeasureMetadata
  }


}
