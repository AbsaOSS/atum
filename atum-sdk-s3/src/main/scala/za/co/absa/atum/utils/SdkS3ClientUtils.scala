package za.co.absa.atum.utils

import software.amazon.awssdk.auth.credentials.{AwsCredentialsProvider, ProfileCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import za.co.absa.atum.core.AtumSdkS3.log

object SdkS3ClientUtils {

  def getLocalProfileCredentialsProvider(credentialsProfileName: String): ProfileCredentialsProvider = {
    val localProfileCredentials = ProfileCredentialsProvider.create(credentialsProfileName)
    log.debug(s"Credentials of local $credentialsProfileName profile =" +
      s" ${localProfileCredentials.resolveCredentials().accessKeyId()}, ${localProfileCredentials.resolveCredentials().secretAccessKey().take(5)}...")

    localProfileCredentials
  }

  def getS3Client(region: Region, credentialsProvider: AwsCredentialsProvider): S3Client = {
    S3Client.builder()
      .region(region)
      .credentialsProvider(credentialsProvider)
      .build()
  }

}
