package za.co.absa.atum.persistence.hdfs

import org.mockito.captor.{ArgCaptor, Captor}
import org.mockito.scalatest.IdiomaticMockito
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{PutObjectRequest, PutObjectResponse, ServerSideEncryption}
import za.co.absa.atum.persistence.s3.ControlMeasuresS3StorerJsonFile
import za.co.absa.atum.persistence.{S3KmsSettings, S3Location, TestResources}
import za.co.absa.atum.utils.FileUtils

import scala.io.Source

class ControlMeasuresS3StorerJsonSpec extends AnyFlatSpec with Matchers with IdiomaticMockito {

  val inputControlMeasure = TestResources.InputInfo.controlMeasure

  "ControlMeasuresS3StorerJsonFile" should "store measurements to json file to S3" in {

    val outputLocation = S3Location(bucketName = "bucket1", "path/to/json.info", region = Region.EU_WEST_2)
    val kmsSettigns = S3KmsSettings("testingKeyId123")
    val mockedS3Client = mock[S3Client]

    implicit val credentialsProvider = DefaultCredentialsProvider.create()

    val storer = new ControlMeasuresS3StorerJsonFile(outputLocation, kmsSettigns) {
      override def getS3Client: S3Client = mockedS3Client
    }

    // mock S3 response
    Mockito.when(mockedS3Client.putObject(ArgumentMatchers.any[PutObjectRequest], ArgumentMatchers.any[RequestBody]))
      .thenReturn(mock[PutObjectResponse]) // anything non-throwing
    val loadedControlMeasure = storer.store(inputControlMeasure)

    // verify request content
    val putRequestCaptor: Captor[PutObjectRequest] = ArgCaptor[PutObjectRequest]
    val requestBodyCaptor: Captor[RequestBody] = ArgCaptor[RequestBody]

    Mockito.verify(mockedS3Client).putObject(putRequestCaptor.capture, requestBodyCaptor.capture)
    val (capturedPutRequest, capturedRequestBody) = (putRequestCaptor.value, requestBodyCaptor.value)

    capturedPutRequest.bucket shouldBe "bucket1"
    capturedPutRequest.key shouldBe "path/to/json.info"
    capturedPutRequest.ssekmsKeyId shouldBe "testingKeyId123"
    capturedPutRequest.serverSideEncryption() shouldBe ServerSideEncryption.AWS_KMS

    // This expected request body content should be the same as content of this file ( ~inputControlMeasure)
    val sameContentFile: String = TestResources.InputInfo.localPath
    val expectedContent = FileUtils.readFileToString(sameContentFile)

    val requestDataContent = Source.fromInputStream(capturedRequestBody.contentStreamProvider().newStream()).mkString
    TestResources.filterWhitespaces(requestDataContent) shouldBe TestResources.filterWhitespaces(expectedContent)

  }

}
