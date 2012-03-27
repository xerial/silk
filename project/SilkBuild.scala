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

package silk

import java.io.File
import sbt._
import Keys._
import sbt.classpath.ClasspathUtilities


object SilkBuild extends Build {

  lazy val commandSettings = Seq(printState)

  val SCALA_VERSION = "2.9.1-1"

  lazy val buildSettings = Defaults.defaultSettings ++ Seq[Setting[_]](
    commands ++= commandSettings,
    organization := "org.xerial.silk",
    organizationName := "Xerial Project",
    organizationHomepage := Some(new URL("http://xerial.org/")),
    version := "0.1-SNAPSHOT",
    description := "Silk: A Scalable Data Processing Platform",
    scalaVersion := SCALA_VERSION,
    publishMavenStyle := true,
    publishArtifact in Test := false,
    publishTo <<= version {
      v: String =>
        val repoPath = "/home/web/maven.xerial.org/repository/" + (if (v.trim.endsWith("SNAPSHOT")) "snapshot" else "artifact")
        Some(Resolver.ssh("Xerial Repo", "www.xerial.org", repoPath)
          as(System.getProperty("user.name"), new File(Path.userHome.absolutePath + "/.ssh/id_rsa")))
    },
//    otherResolvers <++= (publishTo in publishLocal)(_.toList),
//    publishTo in publishLocal := Some(Resolver.file("m2", file(Path.userHome.absolutePath + "/.m2/repository"))),
//    publishLocalConfiguration <<= (packagedArtifacts, publishTo in publishLocal, publishMavenStyle, deliverLocal, ivyLoggingLevel) map { (arts, pTo, mavenStyle, ivyFile, level) =>
//      Classpaths.publishConfig(arts, if (mavenStyle) None else Some(ivyFile), resolverName = if (pTo.isDefined) pTo.get.name else "local", logging = level)
//    },
    pomIncludeRepository := {
      _ => false
    },
    parallelExecution := true,
    crossPaths := false,
    //crossScalaVersions := Seq("2.10.0-M1", "2.9.1-1", "2.9.1"),
    resolvers ++= Seq("Typesafe repository" at "http://repo.typesafe.com/typesafe/releases",
      //"sbt-idea-repo" at "http://mpeltonen.github.com/maven/",
      "UTGB Maven repository" at "http://maven.utgenome.org/repository/artifact/",
      "Xerial Maven repository" at "http://www.xerial.org/maven/repository/artifact",
      "Local Maven repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository"
    ),
    scalacOptions ++= Seq("-encoding", "UTF-8", "-deprecation", "-unchecked"),
    pomExtra := {
      <licenses>
        <license>
          <name>Apache 2</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
        </license>
      </licenses>
        <scm>
          <connection>scm:git:git@github.com:xerial/silk.git</connection>
          <developerConnection>scm:git:git@github.com:xerial/silk.git</developerConnection>
        </scm>
        <properties>
          <scala.version>2.9.1-1</scala.version>
          <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        </properties>
        <developers>
          <developer>
            <id>leo</id>
            <name>Taro L. Saito</name>
            <url>http://www.xerial.org/leo</url>
          </developer>
        </developers>
    }
  )

  val distAllClasspaths = TaskKey[Seq[Classpath]]("dist-all-classpaths")
  val distDependencies = TaskKey[Seq[File]]("dist-dependencies")
  val distLibJars = TaskKey[Seq[File]]("dist-lib-jars")

  lazy val distSettings: Seq[Setting[_]] = Seq(
    distAllClasspaths <<= (thisProjectRef, buildStructure) flatMap allProjects(dependencyClasspath.task in Compile),
    distDependencies <<= distAllClasspaths map {
      _.flatten.map(_.data).filter(ClasspathUtilities.isArchive).distinct
    },
    distLibJars <<= (thisProjectRef, buildStructure) flatMap allProjects(packageBin.task in Compile)
  )

  def allProjects[T](task: SettingKey[Task[T]])(currentProject: ProjectRef, structure: Load.BuildStructure): Task[Seq[T]] = {
    val projects: Seq[String] = currentProject.project +: childProjectNames(currentProject, structure)
    projects flatMap {
      task in LocalProject(_) get structure.data
    } join
  }

  def childProjectNames(currentProject: ProjectRef, structure: Load.BuildStructure): Seq[String] = {
    val children = Project.getProject(currentProject, structure).toSeq.flatMap(_.aggregate)
    children flatMap {
      ref =>
        ref.project +: childProjectNames(ref, structure)
    }
  }

  object Dependencies {

    val classWorld = "org.codehaus.plexus" % "plexus-classworlds" % "2.4"

    val testLib = Seq(
      "junit" % "junit" % "4.10" % "test",
      "org.scalatest" %% "scalatest" % "1.7.1" % "test",
      "org.hamcrest" % "hamcrest-core" % "1.3.RC2" % "test"
    )

    val bootLib = Seq(
      classWorld
    )

    val networkLib = Seq(
      "io.netty" % "netty" % "3.3.0.Final",
      "com.typesafe.akka" % "akka-actor" % "2.0-M4",
      "com.typesafe.akka" % "akka-remote" % "2.0-M4"
    )

    val reflectionLib = Seq(
      "org.javassist" % "javassist" % "3.15.0-GA"
    )

    val scalap = "org.scala-lang" % "scalap" % SCALA_VERSION
    val xerialCore = "org.xerial" % "xerial-core" % "2.0.2"
  }

  import Dependencies._


  private val dependentScope = "test->test;compile->compile"

  lazy val root = Project(
    id = "silk",
    base = file("."),
    aggregate = Seq[ProjectReference](core, text, weaver, workflow, genomeLens),
    settings = buildSettings ++ distSettings ++ Release.settings
      ++ Seq(packageDistTask)
      ++ Seq(libraryDependencies ++= bootLib)
  )

  lazy val core = Project(
    id = "silk-core",
    base = file("silk-core"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= testLib ++ Seq(xerialCore, scalap)
    )
  )

  lazy val text = Project(id = "silk-text", base = file("silk-text"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= testLib
    )
  ) dependsOn (core % dependentScope)

  lazy val weaver = Project(id = "silk-weaver", base = file("silk-weaver"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= testLib ++ networkLib
    )
  ) dependsOn (core % dependentScope)

  lazy val workflow = Project(id = "silk-workflow", base = file("silk-workflow"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= testLib
    )
  ) dependsOn (core % dependentScope)

  lazy val genomeLens = Project(id = "genome-lens", base = file("genome-lens"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= testLib
    )
  ) dependsOn (weaver % dependentScope)

  def hello = Command.command("hello") {
    state =>
      println("Hello silk!")
      state
  }


  lazy val copyDependencies = TaskKey[Unit]("copy-dependencies")

  def copyDepTask = copyDependencies <<= (update, crossTarget, scalaVersion) map {
    (updateReport, out, scalaVer) =>
      updateReport.allFiles foreach {
        srcPath =>
          val destPath = out / "lib" / srcPath.getName
          IO.copyFile(srcPath, destPath, preserveLastModified = true)
      }
  }

  lazy val packageDist: TaskKey[File] = TaskKey[File]("package-dist")

  def packageDistTask: Setting[Task[File]] = packageDist <<= (version, distLibJars, distDependencies, streams, target, dependencyClasspath in Runtime, classDirectory in Compile, baseDirectory) map {
    (ver, libs, depJars, out, target, dependencies, classDirectory, base) => {

      val distDir = target / "dist"

      out.log.info("output dir: " + distDir)
      IO.delete(distDir)
      distDir.mkdirs()

      out.log.info("Copy libraries")
      val libDir = distDir / "lib"
      libDir.mkdirs()
      (libs ++ depJars).foreach(l => IO.copyFile(l, libDir / l.getName))

      out.log.info("Create bin folder")
      val binDir = distDir / "bin"
      binDir.mkdirs()
      IO.copyDirectory(base / "src/script", binDir)

      out.log.info("Generating version info")
      IO.write(distDir / "VERSION", ver)
      out.log.info("done.")

      distDir
    }
  }

  def printState = Command.command("print-state") {
    state =>
      import state._
      def show[T](s: Seq[T]) =
        s.map("'" + _ + "'").mkString("[", ", ", "]")
      println(definedCommands.size + " registered commands")
      println("commands to run: " + show(remainingCommands))
      println()

      println("original arguments: " + show(configuration.arguments))
      println("base directory: " + configuration.baseDirectory)
      println()

      println("sbt version: " + configuration.provider.id.version)
      println("Scala version (for sbt): " + configuration.provider.scalaProvider.version)
      println()

      val extracted = Project.extract(state)
      import extracted._
      println("Current build: " + currentRef.build)
      println("Current project: " + currentRef.project)
      println("Original setting count: " + session.original.size)
      println("Session setting count: " + session.append.size)
      println(allDependencies get extracted.structure.data)
      state
  }
}








