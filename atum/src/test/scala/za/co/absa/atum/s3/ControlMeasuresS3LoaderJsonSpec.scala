package za.co.absa.atum.s3

import org.mockito.scalatest.IdiomaticMockito
import org.mockito.{ArgumentMatcher, ArgumentMatchers, Mockito}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, GetObjectResponse}
import za.co.absa.atum.persistence.s3.ControlMeasuresS3LoaderJsonFile
import za.co.absa.atum.persistence.{S3Location, TestResources}
import za.co.absa.atum.utils.FileUtils

class ControlMeasuresS3LoaderJsonSpec extends AnyFlatSpec with Matchers with IdiomaticMockito {

  val inputPath: String = TestResources.InputInfo.localPath
  val expectedInputControlMeasure = TestResources.InputInfo.controlMeasure

  "ControlMeasuresS3LoaderJsonFile" should "load json file from (mocked) S3" in {

    val inputLocation = S3Location(bucketName = "bucket1", "path/to/json.info", region = Region.EU_WEST_2)
    val mockedS3Client = mock[S3Client]
    val mockedRequest: ResponseBytes[GetObjectResponse] = mock[ResponseBytes[GetObjectResponse]]

    val loader = new ControlMeasuresS3LoaderJsonFile(inputLocation){
      override def getS3Client: S3Client = mockedS3Client
    }

    val mockedS3Data = FileUtils.readFileToString(inputPath)

    Mockito.when(mockedS3Client.getObjectAsBytes(ArgumentMatchers.any[GetObjectRequest]())).thenReturn(mockedRequest)
    Mockito.when(mockedRequest.asUtf8String()).thenReturn(mockedS3Data)

    val loadedControlMeasure = loader.load()
    loadedControlMeasure shouldBe expectedInputControlMeasure
  }

  def argMatch[T](func: T => Boolean): T = {
    ArgumentMatchers.argThat(new ArgumentMatcher[T] {
      override def matches(param: T): Boolean = {
        func(param)
      }
    })
  }

}
