package za.co.absa.atum.utils

import org.scalatest.flatspec.AnyFlatSpec
import S3LocationUtils.StringS3LocationExt
import org.scalatest.matchers.should.Matchers

class S3UtilsSpec extends AnyFlatSpec with Matchers {

  val validPathsWithExpectedLocations = Seq(
    // (path, expected parsed value)
    ("s3://mybucket-123/path/to/file.ext", SimpleS3Location("s3", "mybucket-123", "path/to/file.ext")),
    ("s3n://mybucket-123/path/to/ends/with/slash/", SimpleS3Location("s3n","mybucket-123", "path/to/ends/with/slash/")),
    ("s3a://mybucket-123.asdf.cz/path-to-$_file!@#$.ext", SimpleS3Location("s3a", "mybucket-123.asdf.cz", "path-to-$_file!@#$.ext"))
  )

  val invalidPaths = Seq(
    "s3x://mybucket-123/path/to/file/on/invalid/prefix",
    "s3://bb/some/path/but/bucketname/too/short"
  )

  "S3Utils.StringS3LocationExt" should "parse S3 path from String using toS3Location" in {
    validPathsWithExpectedLocations.foreach { case (path, expectedLocation) =>
      path.toS3Location shouldBe Some(expectedLocation)
    }
  }

  it should "find no valid S3 path when parsing invalid S3 path from String using toS3Location" in {
    invalidPaths.foreach {
      _.toS3Location shouldBe None
    }
  }

  it should "fail parsing invalid S3 path from String using toS3LocationOrFail" in {
    invalidPaths.foreach { path =>
      assertThrows[IllegalArgumentException] {
        path.toS3LocationOrFail
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
