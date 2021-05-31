package za.co.absa.atum.utils

import org.scalatest.flatspec.AnyFlatSpec
import za.co.absa.commons.version.Version

import scala.util.{Failure, Success, Try}

class BuildPropertiesSpec extends AnyFlatSpec  {
  private val version = BuildProperties.buildVersion
  private val name = BuildProperties.projectName

  "Project version" should "be parsable by the semVer" in {
    Try {
      Version.asSemVer(version)
    } match {
      case Success(_) => succeed
      case Failure(exception) => fail(exception.getMessage, exception.getCause)
    }
  }

  "Project Name" should "start with atum and scala version" in {
    assert(name.matches("""^atum_(2\.11|2\.12)$"""))
  }
}
