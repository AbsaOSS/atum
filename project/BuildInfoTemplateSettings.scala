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

import sbt.Keys._
import sbt._

// heavily inspired by Cobrix
object BuildInfoTemplateSettings {

  lazy val populateBuildInfoTemplate: Seq[Def.Setting[_]] = Seq(
    Compile / unmanagedResources / excludeFilter := excludeTemplateResource.value,
    Compile / resourceGenerators += populateResourceTemplate.taskValue
  )

  private val excludeTemplateResource = Def.setting {
    val propsTemplate = ((Compile / resourceDirectory).value / "atum_build.properties").getCanonicalPath
    new SimpleFileFilter(_.getCanonicalPath == propsTemplate)
  }

  private val populateResourceTemplate = Def.task {
    val template = IO.read((Compile / resourceDirectory).value / "atum_build.properties")

    val filledTemplate = template
      .replace("${project.version}", version.value)
      .replace("${project.artifactId}", s"${artifact.value.name}_${scalaBinaryVersion.value}")

    val out = (Compile / resourceManaged).value / "atum_build.properties"
    IO.write(out, filledTemplate)
    Seq(out)
  }

}
