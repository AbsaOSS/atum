package za.co.absa.atum.persistence.s3

import org.mockito.captor.{ArgCaptor, Captor}
import org.mockito.scalatest.IdiomaticMockito
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, GetObjectResponse}
import za.co.absa.atum.persistence.{SimpleS3LocationWithRegion, TestResources}
import za.co.absa.atum.utils.FileUtils

class ControlMeasuresSdkS3LoaderJsonSpec extends AnyFlatSpec with Matchers with IdiomaticMockito {

  val expectedInputControlMeasure = TestResources.InputInfo.controlMeasure

  "ControlMeasuresS3LoaderJsonFile" should "load measurements from json file from (mocked) S3" in {

    val inputLocation = SimpleS3LocationWithRegion(bucketName = "bucket1", "path/to/json.info", region = Region.EU_WEST_2)
    val mockedS3Client = mock[S3Client]
    val mockedRequest: ResponseBytes[GetObjectResponse] = mock[ResponseBytes[GetObjectResponse]]

    implicit val credentialsProvider = DefaultCredentialsProvider.create()
    val loader = new ControlMeasuresSdkS3LoaderJsonFile(inputLocation) {
      override def getS3Client: S3Client = mockedS3Client
    }

    // This file is mocked to be read from in S3
    val inputFilePath: String = TestResources.InputInfo.localPath
    val mockedS3Data = FileUtils.readFileToString(inputFilePath)

    // mock S3 response
    Mockito.when(mockedS3Client.getObjectAsBytes(ArgumentMatchers.any[GetObjectRequest]())).thenReturn(mockedRequest)
    Mockito.when(mockedRequest.asUtf8String()).thenReturn(mockedS3Data)
    val loadedControlMeasure = loader.load()

    // verify request content
    val getRequestCaptor: Captor[GetObjectRequest] = ArgCaptor[GetObjectRequest]
    Mockito.verify(mockedS3Client).getObjectAsBytes(getRequestCaptor.capture)
    val capturedGetRequest = getRequestCaptor.value

    capturedGetRequest.bucket shouldBe "bucket1"
    capturedGetRequest.key shouldBe "path/to/json.info"

    // verify returned value
    loadedControlMeasure shouldBe expectedInputControlMeasure
  }

}
