package za.co.absa.atum.utils

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.{S3Client, S3ClientBuilder}
import za.co.absa.atum.core.Atum.log

object S3ClientUtils {

  def getS3ClientWithLocalProfile(region: Region, profileName: String): S3Client = {
    val localProfileCredentials = ProfileCredentialsProvider.create(profileName)
    log.debug(s"Credentials of local $profileName profile =" +
      s" ${localProfileCredentials.resolveCredentials().accessKeyId()}, ${localProfileCredentials.resolveCredentials().secretAccessKey().take(5)}...")

    getS3Client(region, Some(localProfileCredentials))
  }

  def getS3Client(region: Region, credentialsProvider: Option[ProfileCredentialsProvider] = None): S3Client = {
    S3Client.builder()
      .region(region)
      .applyCredentialsProviderIfDefined(credentialsProvider)
      .build()
  }

  implicit class S3ClientBuilderExt(s3ClientBuilder: S3ClientBuilder) {
    /**
     * Universal conditional S3ClientBuilder=>S3ClientBuilder apply method
     *
     * @param condition `fn` will be applied when true, not applied when false
     * @param fn        full
     * @return original object
     */
    def applyIf(condition: Boolean, fn: S3ClientBuilder => S3ClientBuilder): S3ClientBuilder = {
      if (condition) {
        fn(s3ClientBuilder)
      } else
        s3ClientBuilder
    }

    /**
     * Apply `optionalCredentialsProvider` if defined
     *
     * @param optionalCredentialsProvider
     * @return original object
     */
    def applyCredentialsProviderIfDefined(optionalCredentialsProvider: Option[ProfileCredentialsProvider]): S3ClientBuilder = {
      optionalCredentialsProvider.fold(s3ClientBuilder)(s3ClientBuilder.credentialsProvider)
    }
  }

}
