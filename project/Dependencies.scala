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

import sbt._

object Dependencies {

  object Versions {
    // TODO alternate for multiversion build; see Issue #121
    val json4s = "3.5.3" // use 3.7.0-M5 for Spark 3+
    val hadoop = "2.8.5" // use 3.2 for Spark 3+

    val absaCommons = "0.0.27"
    val typesafeConfig = "1.4.1"
    val mockitoScala = "1.15.0"
  }

  // TODO alternate for multiversion build (hint: getScalaDependency(scalaVersion.value) in cobrix ); see Issue #121
  lazy val sparkCore = "org.apache.spark" %% "spark-core" % "2.4.7" % Provided
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % "2.4.7" % Provided

  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.9" % Test

  lazy val json4sExt = "org.json4s" %% "json4s-ext" % Versions.json4s
  lazy val json4sCore = "org.json4s" %% "json4s-core" % Versions.json4s % Provided
  lazy val json4sJackson = "org.json4s" %% "json4s-jackson" % Versions.json4s % Provided
  lazy val json4sNative = "org.json4s" %% "json4s-native" % Versions.json4s % Provided

  lazy val absaCommons = "za.co.absa.commons" %% "commons" % Versions.absaCommons
  lazy val commonsConfiguration = "commons-configuration" % "commons-configuration" % "1.6"
  lazy val apacheCommons = "org.apache.commons" % "commons-lang3" % "3.5"
  lazy val typeSafeConfig = "com.typesafe" % "config" % Versions.typesafeConfig

  lazy val mockitoScala = "org.mockito" %% "mockito-scala" % Versions.mockitoScala % Test
  lazy val mockitoScalaScalatest = "org.mockito" %% "mockito-scala-scalatest" % Versions.mockitoScala % Test
  lazy val hadoopMinicluster = "org.apache.hadoop" % "hadoop-minicluster" % Versions.hadoop % Test

  lazy val sdkS3 = "software.amazon.awssdk" % "s3" % "2.13.65"

  lazy val rootDependencies: Seq[ModuleID] = Seq(
    sparkCore,
    sparkSql,
    scalaTest,
    json4sExt
  )

  lazy val modelDependencies: Seq[ModuleID] = Seq(
    json4sCore,
    json4sJackson,
    json4sNative
  )

  lazy val coreDependencies: Seq[ModuleID] = Seq(
    absaCommons,
    commonsConfiguration,
    apacheCommons,
    typeSafeConfig,

    mockitoScala,
    mockitoScalaScalatest,
    hadoopMinicluster,
  )

  lazy val s3sdkExtensionDependencies: Seq[ModuleID] = Seq(
    absaCommons,
    sdkS3,
    mockitoScala,
    mockitoScalaScalatest
  )

}
