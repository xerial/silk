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
import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.{MultiJvm}

object SilkBuild extends Build {

  val SCALA_VERSION = "2.10.1"

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

  lazy val buildSettings = Defaults.defaultSettings ++ Unidoc.settings ++ releaseSettings ++  SbtMultiJvm.multiJvmSettings ++ Seq[Setting[_]](
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
    testOptions in Test <+= (target in Test) map {
      t => Tests.Argument(TestFrameworks.ScalaTest, "junitxml(directory=\"%s\")".format(t /"test-reports" ), "stdout")
    },
    compile in MultiJvm <<= (compile in MultiJvm) triggeredBy (compile in Test),
    executeTests in Test <<= ((executeTests in Test), (executeTests in MultiJvm)) map {
      case ((_, testResults), (_, multiJvmResults)) =>
        val results = testResults ++ multiJvmResults
        (Tests.overall(results.values), results)
    },
    resolvers ++= Seq(
      "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Sonatype shapshot repo" at "https://oss.sonatype.org/content/repositories/snapshots/"
    ),
    parallelExecution := true,
    parallelExecution in Test := false,
    crossPaths := false,
    scalacOptions ++= Seq("-encoding", "UTF-8", "-deprecation", "-unchecked", "-target:jvm-1.6", "-feature"),
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

  import Dependencies._


  private val dependentScope = "test->test;compile->compile"

  lazy val root = Project(
    id = "silk",
    base = file("."),
    settings = buildSettings ++ packSettings ++ Seq(
      description := "Silk root project",
      // do not publish the root project
      packExclude := Seq("silk"),
      packMain := Map("silk" -> "xerial.silk.SilkMain"),
      publish := {},
      publishLocal := {}
    )
  ) aggregate(silk, xerialCore, xerialLens, xerialCompress, xerialMacro)

  lazy val silk = Project(
    id = "silk-core",
    base = file("silk"),
    settings = buildSettings ++ Seq(
      description := "Silk is a scalable data processing platform",
      libraryDependencies ++= testLib ++ clusterLib
    )
  ) dependsOn(xerialCore % dependentScope, xerialLens, xerialCompress) configs(MultiJvm)



  lazy val xerial = RootProject(file("xerial"))
  lazy val xerialCore = ProjectRef(file("xerial"), "xerial-core")
  lazy val xerialLens = ProjectRef(file("xerial"), "xerial-lens")
  lazy val xerialCompress = ProjectRef(file("xerial"), "xerial-compress")
  lazy val xerialMacro = ProjectRef(file("xerial"), "xerial-macro")


  object Dependencies {

    val testLib = Seq(
      "junit" % "junit" % "4.10" % "test",
      "org.scalatest" %% "scalatest" % "2.0.M5b" % "test",
      "org.scalacheck" % "scalacheck_2.10" % "1.10.0" % "test",
      "com.typesafe.akka" %% "akka-testkit" % "2.1.2" % "test",
      "com.typesafe.akka" %% "akka-remote-tests-experimental" % "2.1.2" % "test"
    )

    val clusterLib = Seq(
      "org.apache.zookeeper" % "zookeeper" % "3.4.3" excludeAll(
        ExclusionRule(organization="org.jboss.netty"),
        ExclusionRule(organization="com.sun.jdmk"),
        ExclusionRule(organization="com.sun.jmx"),
        ExclusionRule(organization="javax.jms")),
      "org.ow2.asm" % "asm-all" % "4.1",
      //"io.netty" % "netty" % "3.6.1.Final",
      "org.xerial.snappy" % "snappy-java" % "1.0.5-M3",
      "org.xerial" % "larray" % "0.1-SNAPSHOT",
      "com.netflix.curator" % "curator-recipes" % "1.2.3",
      "com.netflix.curator" % "curator-test" % "1.2.3",
      "org.slf4j" % "slf4j-api" % "1.6.4",
      "org.slf4j" % "slf4j-log4j12" % "1.6.4",
      "com.typesafe.akka" %% "akka-actor" % "2.2-M2",
      "com.typesafe.akka" %% "akka-remote" % "2.2-M2",
      "com.google.protobuf" % "protobuf-java" % "2.4.1",
      "com.esotericsoftware.kryo" % "kryo" % "2.20"
    )

  }

}








