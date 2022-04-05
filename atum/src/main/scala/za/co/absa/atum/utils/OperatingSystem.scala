/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.atum.utils


object OperatingSystem {

  // adapted from https://stackoverflow.com/a/31547504/1773349

  object OperatingSystems extends Enumeration {
    val WINDOWS, LINUX, MAC, SOLARIS, OTHER = Value
  }

  def getOsByOsName(osName: String): OperatingSystems.Value = {
    import za.co.absa.atum.utils.OperatingSystem.OperatingSystems._
    osName.toLowerCase match {
      case os if os.contains("win") => WINDOWS
      case os if os.contains("nix") || os.contains("nux") || os.contains("aix") => LINUX
      case os if os.contains("mac") => MAC
      case os if os.contains("sunos") => SOLARIS
      case _ => OTHER
    }
  }

  def getCurrentOs: OperatingSystems.Value = {
    getOsByOsName(System.getProperty("os.name"))
  }

}
