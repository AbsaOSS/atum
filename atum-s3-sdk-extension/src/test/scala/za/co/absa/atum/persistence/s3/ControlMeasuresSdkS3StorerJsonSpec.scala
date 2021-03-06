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

package za.co.absa.atum.persistence.s3

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
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.atum.persistence.TestResources
import za.co.absa.atum.utils.FileUtils

import scala.io.Source

class ControlMeasuresSdkS3StorerJsonSpec extends AnyFlatSpec with Matchers with IdiomaticMockito {

  val inputControlMeasure: ControlMeasure = TestResources.InputInfo.controlMeasure

  "ControlMeasuresS3StorerJsonFile" should "store measurements to json file to S3" in {

    val outputLocation = SimpleS3LocationWithRegion("s3", "bucket1", "path/to/json.info", Region.EU_WEST_2)
    val kmsSettigns = S3KmsSettings("testingKeyId123")
    val mockedS3Client = mock[S3Client]

    implicit val credentialsProvider: DefaultCredentialsProvider = DefaultCredentialsProvider.create()

    val storer = new ControlMeasuresSdkS3StorerJsonFile(outputLocation, kmsSettigns) {
      override def getS3Client: S3Client = mockedS3Client
    }

    // mock S3 response
    Mockito.when(mockedS3Client.putObject(ArgumentMatchers.any[PutObjectRequest], ArgumentMatchers.any[RequestBody]))
      .thenReturn(mock[PutObjectResponse]) // anything non-throwing
    storer.store(inputControlMeasure)

    // verify request content
    val putRequestCaptor: Captor[PutObjectRequest] = ArgCaptor[PutObjectRequest]
    val requestBodyCaptor: Captor[RequestBody] = ArgCaptor[RequestBody]

    Mockito.verify(mockedS3Client).putObject(putRequestCaptor.capture, requestBodyCaptor.capture)
    val (capturedPutRequest, capturedRequestBody) = (putRequestCaptor.value, requestBodyCaptor.value)

    capturedPutRequest.bucket shouldBe "bucket1"
    capturedPutRequest.key shouldBe "path/to/json.info"
    capturedPutRequest.ssekmsKeyId shouldBe "testingKeyId123"
    capturedPutRequest.serverSideEncryption() shouldBe ServerSideEncryption.AWS_KMS

    // This expected request body content should be the same as content of this file (conforms to `inputControlMeasure`)
    val sameContentFile: String = TestResources.InputInfo.localPath
    val expectedContent = FileUtils.readFileToString(sameContentFile)

    val requestDataContent = Source.fromInputStream(capturedRequestBody.contentStreamProvider().newStream()).mkString
    TestResources.filterWhitespaces(requestDataContent) shouldBe TestResources.filterWhitespaces(expectedContent)

  }

}
