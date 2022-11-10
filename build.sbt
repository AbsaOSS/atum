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

ThisBuild / organization := "za.co.absa"
ThisBuild / name         := "atum"

import Dependencies._
import BuildInfoTemplateSettings._

lazy val scala211 = "2.11.12"
lazy val scala212 = "2.12.15"

ThisBuild / scalaVersion := scala211  // default version
ThisBuild / crossScalaVersions := Seq(scala211, scala212)

lazy val printSparkScalaVersion = taskKey[Unit]("Print Spark and Scala versions that Atum is being built for.")
ThisBuild / printSparkScalaVersion := {
  val log = streams.value.log
  val sparkVer = sparkVersionForScala(scalaVersion.value)
  log.info(s"Building with Spark ${sparkVer}, Scala ${scalaVersion.value}")
}

lazy val parent = (project in file("."))
  .aggregate(model, core, s3sdkExtension)
  .settings(
    name := "atum-parent",
    libraryDependencies ++= rootDependencies(scalaVersion.value),
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint"),
    // no default main defined class for Atum; future use: assembly / mainClass := Some("za.co.absa.atum.SomeApp")
    assembly / test := (Test / test).value,
    mergeStrategy,
    publish / skip := true
    // TODO no assembly needed here?
  )

lazy val jacocoModelReportSettings = Seq(
  jacocoReportSettings := JacocoReportSettings(
    s"atum:model Jacoco Report",
    None,
    JacocoThresholds(),
    Seq(JacocoReportFormats.HTML, JacocoReportFormats.XML),
    "utf-8")
)

lazy val model = project // no need to define file, because path is same as val name
  .settings(
    name := "atum-model",
    libraryDependencies ++= (rootDependencies(scalaVersion.value) ++ modelDependencies(scalaVersion.value)),
    assembly / test := (Test / test).value,
    (Compile / compile) := ((Compile / compile) dependsOn printSparkScalaVersion).value, // printSparkScalaVersion is run with compile
    mergeStrategy
  )
  .settings(jacocoModelReportSettings: _*)

lazy val jacocoCoreReportSettings = Seq(
  jacocoReportSettings := JacocoReportSettings(
    s"atum:core Jacoco Report",
    None,
    JacocoThresholds(),
    Seq(JacocoReportFormats.HTML, JacocoReportFormats.XML),
    "utf-8"),
  jacocoExcludes := Seq(
//    "za.co.absa.atum.core.ControlType.DistinctCount*", // class and related objects
//    "za.co.absa.atum.core.ControlFrameworkState" // class only
  )
)

lazy val core = (project in file("atum"))
  .settings(
    name := "atum",
    libraryDependencies ++= (rootDependencies(scalaVersion.value) ++ coreDependencies(scalaVersion.value)),
    assembly / test := (Test / test).value,
    (Compile / compile) := ((Compile / compile) dependsOn printSparkScalaVersion).value, // printSparkScalaVersion is run with compile
    mergeStrategy,
    populateBuildInfoTemplate // to get correct replacements for ${project.artifactId} and ${project.version} in atum_build.properties,
  )
  .settings(jacocoCoreReportSettings)
  .dependsOn(model)

lazy val jacocos3SDKExtReportSettings = Seq(
  jacocoReportSettings := JacocoReportSettings(
    s"atum:s3sdkExtension Jacoco Report",
    None,
    JacocoThresholds(),
    Seq(JacocoReportFormats.HTML, JacocoReportFormats.XML),
    "utf-8")
)

lazy val s3sdkExtension = (project in file("atum-s3-sdk-extension"))
  .settings(
    name := "atum-s3-sdk-extension",
    libraryDependencies ++= (rootDependencies(scalaVersion.value) ++ s3sdkExtensionDependencies),
    scalacOptions += "-target:jvm-1.8", // AWS S3 SDK requires 1.8 level for static methods in interfaces
    assembly / test := (Test / test).value,
    (Compile / compile) := ((Compile / compile) dependsOn printSparkScalaVersion).value, // printSparkScalaVersion is run with compile
    mergeStrategy
  )
  .settings(jacocos3SDKExtReportSettings: _*)
  .dependsOn(core)

lazy val jacocoExamplesReportSettings = Seq(
  jacocoReportSettings := JacocoReportSettings(
    s"atum:examples Jacoco Report",
    None,
    JacocoThresholds(),
    Seq(JacocoReportFormats.HTML, JacocoReportFormats.XML),
    "utf-8")
)

lazy val examples = (project in file("examples"))
  .settings(jacocoExamplesReportSettings: _*)
  .settings(
    name := "examples",
    libraryDependencies ++= (rootDependencies(scalaVersion.value) ++ examplesDependencies),
    assembly / test := (Test / test).value,
    Test / parallelExecution := false, // Atum Control framework could attempt to double-initialize and fail
    (Compile / compile) := ((Compile / compile) dependsOn printSparkScalaVersion).value, // printSparkScalaVersion is run with compile
    mergeStrategy
  )
  .dependsOn(core)

lazy val jacocoS3sdkExamplesReportSettings = Seq(
  jacocoReportSettings := JacocoReportSettings(
    s"atum:s3sdkExamples Jacoco Report",
    None,
    JacocoThresholds(),
    Seq(JacocoReportFormats.HTML, JacocoReportFormats.XML),
    "utf-8")
)

lazy val s3sdkExamples = (project in file("examples-s3-sdk-extension"))
  .settings(
    name := "examples-s3-sdk-extension",
    libraryDependencies ++= (rootDependencies(scalaVersion.value) ++ s3sdkExtensionDependencies),
    scalacOptions += "-target:jvm-1.8", // AWS S3 SDK requires 1.8 level for static methods in interfaces
    assembly / test := {}, // skipping tests for s3sdk, because one needs specific setup to run it (intent: run manually)
    (Compile / compile) := ((Compile / compile) dependsOn printSparkScalaVersion).value, // printSparkScalaVersion is run with compile
    mergeStrategy,
  )
  .settings(jacocoS3sdkExamplesReportSettings: _*)
  .dependsOn(s3sdkExtension, examples)

val mergeStrategy: Def.SettingsDefinition = assembly / assemblyMergeStrategy  := {
  case PathList("META-INF", _) => MergeStrategy.discard
  case "application.conf"      => MergeStrategy.concat
  case "reference.conf"        => MergeStrategy.concat
  case _                       => MergeStrategy.first
}
