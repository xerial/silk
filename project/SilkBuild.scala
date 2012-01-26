/*
 * Copyright 2012 Taro L. Saito
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt._
import Keys._


object SilkBuild extends Build {


  val buildSettings = Defaults.defaultSettings ++ Seq(
    organization := "org.xerial.silk",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.9.1",
    crossScalaVersions := Seq("2.10.0-M1", "2.9.1", "2.9.0-1", "2.8.1"),
    resolvers ++= Seq("Typesafe repository" at "http://repo.typesafe.com/typesafe/releases"),
    scalacOptions ++= Seq("-encoding", "UTF-8", "-deprecation", "-unchecked")
  )

  object Dependencies {
    val testLib = Seq(
      "junit" % "junit" % "4.10" % "test",
      "org.scalatest" %% "scalatest" % "1.6.1" % "test",
      "org.hamcrest" % "hamcrest-core" % "1.3.RC2" % "test"
    )

    val networkLib = Seq(
      "io.netty" % "netty" % "3.3.0.Final",
      "com.typesafe.akka" % "akka-actor" % "2.0-M2",
      "com.typesafe.akka" % "akka-remote" % "2.0-M2"
    )

    val javassist = Seq(
      "org.javassist" % "javassist" % "3.15.0-GA"
    )
  }

  import Dependencies._

  lazy val root = Project(
    id = "silk",
    base = file("."),
    aggregate = Seq[ProjectReference](core, model, lens, parser, store, weaver),
    settings = buildSettings ++ Seq(commands ++= Seq(hello))
  )

  lazy val core = Project(
    id = "silk-core",
    base = file("silk-core"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= testLib ++ javassist
    )
  )

  lazy val model = Project(id = "silk-model", base = file("silk-model"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= testLib
    )
  ) dependsOn (core % "test->test;compile->compile")

  lazy val lens = Project(id = "silk-lens", base = file("silk-lens"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= testLib ++ javassist
    )
  ) dependsOn (core % "test->test;compile->compile")

  lazy val parser = Project(id = "silk-parser", base = file("silk-parser"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= testLib
    )
  ) dependsOn (core % "test->test;compile->compile")

  lazy val store = Project(id = "silk-store", base = file("silk-store"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= testLib
    )
  ) dependsOn (core % "test->test;compile->compile")

  lazy val weaver = Project(id = "silk-weaver", base = file("silk-weaver"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= testLib ++ networkLib
    )
  ) dependsOn (core % "test->test;compile->compile")

  def hello = Command.command("hello") {
    state =>
      println("Hello silk!")
      state
  }


}