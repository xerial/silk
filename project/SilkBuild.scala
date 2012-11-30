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
import sbtrelease.ReleasePlugin._
import scala.Some
import sbt.ExclusionRule
import xerial.sbt.Pack._

object SilkBuild extends Build {

  val SCALA_VERSION = "2.9.2"

  private def profile = System.getProperty("xerial.profile", "default")
  private def isWindows = System.getProperty("os.name").contains("Windows")


  def releaseResolver(v: String): Option[Resolver] = {
    profile match {
      case "default" => {
        val nexus = "https://oss.sonatype.org/"
        if (v.trim.endsWith("SNAPSHOT"))
          Some("snapshots" at nexus + "content/repositories/snapshots")
        else
          Some("releases" at nexus + "service/local/staging/deploy/maven2")
      }
      case p => {
        scala.Console.err.println("unknown xerial.profile '%s'".format(p))
        None
      }
    }
  }

  lazy val buildSettings = Defaults.defaultSettings ++ Unidoc.settings ++ releaseSettings ++ Seq[Setting[_]](
    organization := "org.xerial.silk",
    organizationName := "Silk Project",
    organizationHomepage := Some(new URL("http://xerial.org/")),
    description := "Silk: A Scalale Data Processing Platform",
    scalaVersion := SCALA_VERSION,
    publishMavenStyle := true,
    publishArtifact in Test := false,
    publishTo <<= version { v => releaseResolver(v) },
    pomIncludeRepository := {
      _ => false
    },
    parallelExecution := true,
    crossPaths := false,
    scalacOptions ++= Seq("-encoding", "UTF-8", "-deprecation", "-unchecked", "-target:jvm-1.5"),
    pomExtra := {
      <url>http://xerial.org/</url>
        <licenses>
          <license>
            <name>Apache 2</name>
            <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
          </license>
        </licenses>
        <scm>
          <connection>scm:git:github.com/xerial/silk.git</connection>
          <developerConnection>scm:git:git@github.com:xerial/silk.git</developerConnection>
          <url>github.com/xerial/silk.git</url>
        </scm>
        <properties>
          <scala.version>
            {SCALA_VERSION}
          </scala.version>
          <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        </properties>
        <developers>
          <developer>
            <id>leo</id>
            <name>Taro L. Saito</name>
            <url>http://xerial.org/leo</url>
          </developer>
        </developers>
    }
  )

  import Dist._
  import Dependencies._


  private val dependentScope = "test->test;compile->compile"

  lazy val root = Project(
    id = "silk-root",
    base = file("."),
    settings = buildSettings ++ packSettings ++ Seq(
      description := "Silk root project",
      // do not publish the root project
      packExclude := Seq("silk-root"),
      packMain := Map("silk" -> "xerial.silk.SilkMain"),
      publish := {},
      publishLocal := {}
    )
  ) aggregate(silk, xerialCore, xerialLens)

  lazy val silk = Project(
    id = "silk",
    base = file("silk"),
    settings = buildSettings ++ Seq(
      description := "Silk is a scalable data processing platform",
      libraryDependencies ++= testLib ++ clusterLib
    )
  ) dependsOn(xerialCore % dependentScope, xerialLens)

  lazy val xerial = RootProject(file("xerial"))
  lazy val xerialCore = ProjectRef(file("xerial"), "xerial-core")
  lazy val xerialLens = ProjectRef(file("xerial"), "xerial-lens")


  object Dependencies {

    val classWorld = "org.codehaus.plexus" % "plexus-classworlds" % "2.4" % "provided"

    val testLib = Seq(
      "junit" % "junit" % "4.10" % "test",
      "org.scalatest" %% "scalatest" % "2.0.M5" % "test"
    )

    val clusterLib = Seq(
      "org.apache.zookeeper" % "zookeeper" % "3.4.3" excludeAll(
        ExclusionRule(organization="com.sun.jdmk"),
        ExclusionRule(organization="com.sun.jmx"),
        ExclusionRule(organization="javax.jms")),
      "io.netty" % "netty" % "3.5.7.Final",
      "org.xerial.snappy" % "snappy-java" % "1.0.5-M3",
      "com.netflix.curator" % "curator-recipes" % "1.2.3",
      "com.netflix.curator" % "curator-test" % "1.2.3" % "test"
      //"com.typesafe.akka" % "akka-actor" % "2.0",
      // "com.typesafe.akka" % "akka-remote" % "2.0"
    )

  }

}








