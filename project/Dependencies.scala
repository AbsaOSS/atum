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
    val spark2 = "2.4.8"
    val spark3 = "3.2.1"

    val json4s_spark2 = "3.5.3"
    val json4s_spark3 = "3.7.0-M11"

    val jackson_spark2 = "2.10.4"
    val jackson_spark3 = "2.12.6"

    val absaCommons = "0.0.27"
    val typesafeConfig = "1.4.1"
    val mockitoScala = "1.15.0"
    val scalatest = "3.2.9"
    val specs2 = "2.5"
    val aws = "2.17.85"

    val apacheCommonsLang3 = "3.5"
    val commonsConfiguration = "1.6"
  }

  // basic idea of crossversion version picking is based on https://github.com/scala/scala-module-dependency-sample

  // this is just for the compile-depended printing task
  def sparkVersionForScala(scalaVersion: String): String = {
    scalaVersion match {
      case _ if scalaVersion.startsWith("2.11") => Versions.spark2
      case _ if scalaVersion.startsWith("2.12") => Versions.spark3
      case _ => throw new IllegalArgumentException("Only Scala 2.11 and 2.12 are currently supported.")
    }
  }

  // general wrapper to simplify s2.11/2.12 version assigning
  def moduleByScala(moduleIdWithoutVersion: String => ModuleID)
                   (scala211Version: String, scala212Version: String)
                   (actualScalaVersion: String): ModuleID = {
    actualScalaVersion match {
      case _ if actualScalaVersion.startsWith("2.11") => moduleIdWithoutVersion.apply(scala211Version)
      case _ if actualScalaVersion.startsWith("2.12") => moduleIdWithoutVersion.apply(scala212Version)
      case _ => throw new IllegalArgumentException("Only Scala 2.11 and 2.12 are currently supported.")
    }
  }

  val sparkCore = moduleByScala("org.apache.spark" %% "spark-core" % _ % Provided)(Versions.spark2, Versions.spark3) _
  val sparkSql = moduleByScala("org.apache.spark" %% "spark-sql" % _ % Provided)(Versions.spark2, Versions.spark3) _

  lazy val scalaTest = "org.scalatest" %% "scalatest" % Versions.scalatest % Test

  val json4sExt = moduleByScala("org.json4s" %% "json4s-ext" % _)(Versions.json4s_spark2, Versions.json4s_spark3) _
  val json4sCore = moduleByScala("org.json4s" %% "json4s-core" % _ % Provided)(Versions.json4s_spark2, Versions.json4s_spark3) _
  val json4sJackson = moduleByScala("org.json4s" %% "json4s-jackson" % _ % Provided)(Versions.json4s_spark2, Versions.json4s_spark3) _
  val json4sNative = moduleByScala("org.json4s" %% "json4s-native" % _ % Provided)(Versions.json4s_spark2, Versions.json4s_spark3)_
  val jacksonDatabind = moduleByScala("com.fasterxml.jackson.core" % "jackson-databind" % _ )(Versions.jackson_spark2, Versions.jackson_spark3) _
  val jacksonScala = moduleByScala("com.fasterxml.jackson.module" %% "jackson-module-scala" % _ )(Versions.jackson_spark2, Versions.jackson_spark3) _

  lazy val absaCommons = "za.co.absa.commons" %% "commons" % Versions.absaCommons
  lazy val commonsConfiguration = "commons-configuration" % "commons-configuration" % Versions.commonsConfiguration
  lazy val apacheCommons = "org.apache.commons" % "commons-lang3" % Versions.apacheCommonsLang3
  lazy val typeSafeConfig = "com.typesafe" % "config" % Versions.typesafeConfig

  lazy val mockitoScala = "org.mockito" %% "mockito-scala" % Versions.mockitoScala % Test
  lazy val mockitoScalaScalatest = "org.mockito" %% "mockito-scala-scalatest" % Versions.mockitoScala % Test

  lazy val scalaTestProvided = "org.scalatest" %% "scalatest" % Versions.scalatest % Provided
  lazy val specs2core = "org.specs2" %% "specs2-core" % Versions.specs2 % Test

  lazy val sdkS3 = "software.amazon.awssdk" % "s3" % Versions.aws

  def rootDependencies(scalaVersion: String): Seq[ModuleID] = Seq(
    jacksonDatabind(scalaVersion),
    jacksonScala(scalaVersion),
    sparkCore(scalaVersion),
    sparkSql(scalaVersion),
    scalaTest,
    json4sExt(scalaVersion)
  )

  def modelDependencies(scalaVersion: String): Seq[ModuleID] = Seq(
    json4sCore(scalaVersion),
    json4sJackson(scalaVersion),
    json4sNative(scalaVersion)
  )

  def coreDependencies(scalaVersion: String): Seq[ModuleID] = Seq(
    absaCommons,
    commonsConfiguration,
    apacheCommons,
    typeSafeConfig,

    mockitoScala,
    mockitoScalaScalatest,
  )

  lazy val examplesDependencies: Seq[ModuleID] = Seq(
    specs2core,
    scalaTestProvided
  )

  lazy val s3sdkExtensionDependencies: Seq[ModuleID] = Seq(
    absaCommons,
    sdkS3,
    mockitoScala,
    mockitoScalaScalatest
  )

}
