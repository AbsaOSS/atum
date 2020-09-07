package za.co.absa.atum.utils

import software.amazon.awssdk.auth.credentials.{AwsCredentialsProvider, ProfileCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import za.co.absa.atum.core.Atum.log
import za.co.absa.atum.persistence.S3Location

object S3Utils {

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

  // hint: https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html#bucketnamingrules
  val S3LocationRx = "s3(?:a|n)?://([-a-z0-9.]{3,63})/(.*)".r

  implicit class StringS3LocationExt(path: String) {

    def toS3Location(withRegion: Region): S3Location = {
      path match {
        case S3LocationRx(bucketName, path) => S3Location(bucketName, path, withRegion)
        case _ => throw new IllegalArgumentException(s"Could not parse S3 Location from $path using rx $S3LocationRx.")
      }
    }

    def isValidS3Path: Boolean = path match {
      case S3LocationRx(_, _) => true
      case _ => false
    }
  }

}
