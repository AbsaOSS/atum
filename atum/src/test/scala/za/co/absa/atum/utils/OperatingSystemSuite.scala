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
import org.scalatest.matchers.should.Matchers
import za.co.absa.atum.utils.OperatingSystem.OperatingSystems

class OperatingSystemSuite extends AnyFlatSpec with Matchers {

  "OperatingSystem util" should "correctly find out OS" in {
    OperatingSystem.getOsByOsName("Windows 10") shouldBe OperatingSystems.WINDOWS
    OperatingSystem.getOsByOsName("Linux") shouldBe OperatingSystems.LINUX
    OperatingSystem.getOsByOsName("Mac OS X") shouldBe OperatingSystems.MAC
    OperatingSystem.getOsByOsName("SunOs") shouldBe OperatingSystems.SOLARIS

    OperatingSystem.getOsByOsName("my own special os") shouldBe OperatingSystems.OTHER
  }
}
