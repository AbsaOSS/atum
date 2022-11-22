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

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.1.0")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.6.0")

addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.1.2") // formerly known as com.jsuereth:sbt-pgp

addSbtPlugin("com.github.sbt" % "sbt-jacoco" % "3.4.0")

addDependencyTreePlugin
