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
