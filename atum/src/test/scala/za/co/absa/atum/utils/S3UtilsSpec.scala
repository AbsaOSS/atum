package za.co.absa.atum.utils

import org.scalatest.flatspec.AnyFlatSpec
import software.amazon.awssdk.regions.Region
import za.co.absa.atum.persistence.S3Location
import S3Utils.StringS3LocationExt
import org.scalatest.matchers.should.Matchers

class S3UtilsSpec extends AnyFlatSpec with Matchers {

  val region1 = Region.EU_WEST_1

  val validPathsWithExpectedLocations = Seq(
    // (path, expected parsed value)
    ("s3://mybucket-123/path/to/file.ext", S3Location("mybucket-123", "path/to/file.ext", region1)),
    ("s3n://mybucket-123/path/to/ends/with/slash/", S3Location("mybucket-123", "path/to/ends/with/slash/", region1)),
    ("s3a://mybucket-123.asdf.cz/path-to-$_file!@#$.ext", S3Location("mybucket-123.asdf.cz", "path-to-$_file!@#$.ext", region1))
  )

  val invalidPaths = Seq(
    "s3x://mybucket-123/path/to/file/on/invalid/prefix",
    "s3://bb/some/path/but/bucketname/too/short"
  )

  "S3Utils.StringS3LocationExt" should "parse S3 path from String using toS3Location" in {
    validPathsWithExpectedLocations.foreach { case (path, expectedLocation) =>
      path.toS3Location(region1) shouldBe expectedLocation
    }
  }

  it should "fail parsing invalid S3 path from String using toS3Location" in {
    invalidPaths.foreach { path =>
      assertThrows[IllegalArgumentException] {
        path.toS3Location(region1)
      }
    }
  }

  it should "check path using isValidS3Path" in {
    validPathsWithExpectedLocations.map(_._1).foreach { path =>
      path.isValidS3Path shouldBe true
    }

    invalidPaths.foreach(_.isValidS3Path shouldBe false)
  }

}
