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

import java.io.File
import java.io.ByteArrayInputStream
import sbt._
import Keys._
import sbt.classpath.ClasspathUtilities
import sbt.Project.Initialize

object SilkBuild extends Build {


  lazy val buildSettings = Defaults.defaultSettings ++ Seq[Setting[_]](
    organization := "org.xerial.silk",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.9.1",
    parallelExecution := true,
    crossScalaVersions := Seq("2.10.0-M1", "2.9.1", "2.9.0-1", "2.8.1"),
    resolvers ++= Seq("Typesafe repository" at "http://repo.typesafe.com/typesafe/releases"),
    scalacOptions ++= Seq("-encoding", "UTF-8", "-deprecation", "-unchecked")
  )

  val distAllClasspaths = TaskKey[Seq[Classpath]]("dist-all-classpaths")
  val distDependencies = TaskKey[Seq[File]]("dist-dependencies")
  val distLibJars = TaskKey[Seq[File]]("dist-lib-jars")

  lazy val distSettings: Seq[Setting[_]] = Seq(
    distAllClasspaths <<= (thisProjectRef, buildStructure) flatMap aggregated(dependencyClasspath.task in Compile),
    distDependencies <<= distAllClasspaths map { _.flatten.map(_.data).filter(ClasspathUtilities.isArchive).distinct },
    distLibJars <<= (thisProjectRef, buildStructure) flatMap aggregated(packageBin.task in Compile)
  )

  def aggregated[T](task: SettingKey[Task[T]])(projectRef: ProjectRef, structure: Load.BuildStructure): Task[Seq[T]] = {
    val projects = aggregatedProjects(projectRef, structure)
    projects flatMap {
      task in LocalProject(_) get structure.data
    } join
  }

  def aggregatedProjects(projectRef: ProjectRef, structure: Load.BuildStructure): Seq[String] = {
    val aggregate = Project.getProject(projectRef, structure).toSeq.flatMap(_.aggregate)
    aggregate flatMap {
      ref =>
        ref.project +: aggregatedProjects(ref, structure)
    }
  }

  object Dependencies {
    val testLib = Seq(
      "junit" % "junit" % "4.10" % "test",
      "org.scalatest" %% "scalatest" % "1.6.1" % "test",
      "org.hamcrest" % "hamcrest-core" % "1.3.RC2" % "test"
    )

    val bootLib = Seq(
      "org.codehaus.plexus" % "plexus-classworlds" % "2.4"
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

  lazy val modules = Seq(core, model, lens, parser, store, weaver)

  lazy val root = Project(
    id = "silk",
    base = file("."),
    aggregate = Seq[ProjectReference](core, model, lens, parser, store, weaver),
    settings = buildSettings ++ distSettings ++ Seq(
      copyDepTask, assemblyJarsTask, packageDistTask
    )
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

  lazy val copyDependencies = TaskKey[Unit]("copy-dependencies")

  def copyDepTask = copyDependencies <<= (update, crossTarget, scalaVersion) map {
    (updateReport, out, scalaVer) =>
      updateReport.allFiles foreach {
        srcPath =>
          val destPath = out / "lib" / srcPath.getName
          IO.copyFile(srcPath, destPath, preserveLastModified = true)
      }
  }

  lazy val packageDist: TaskKey[Unit] = TaskKey[Unit]("package-dist")

  def packageDistTask: Setting[Task[Unit]] = packageDist <<= (distLibJars, distDependencies, streams, target, dependencyClasspath in Runtime, classDirectory in Compile) map {
    (libs, depJars, out, target, dependencies, classDirectory) => {

      val distDir = target / "dist"
      IO.delete(distDir)
      out.log.info("output dir: " + distDir)
      distDir.mkdirs()
      out.log.info(root.delegates.mkString(", "))

      out.log.info("dependencies " + libs.mkString(", "))
      out.log.info("dep jars "  + depJars.mkString(", "))

      out.log.info("done.")
    }
  }


  //  def packageProject(classpath:PathFinder) : Task = {
  //
  //
  //
  //  }

  val assemblyJars = TaskKey[Unit]("assembly-jars", "Creates a deployable set of jars.")

  def assemblyJarsTask = assemblyJars <<=
    (streams, target, mainClass in Compile, dependencyClasspath in Runtime, classDirectory in Compile) map {
      (out, target, main, dependencies, classDirectory) =>
        IO.delete(target / "deps")
        val assemblyJars = new File(target, "/assemblyJars")
        val manifest_mf = new File(assemblyJars, "META-INF/MANIFEST.MF")
        def allFiles(dir: File): Array[File] = {
          val entries = dir.listFiles
          if (entries == null)
            return Array()
          entries.filter(_.isFile) ++
            entries.filter(_.isDirectory).flatMap(allFiles(_))
        }
        def relPath(file: File): String = {
          file.toString.replace(assemblyJars.toString + "/", "")
        }
        out.log.info("Assemble modules...")
        out.log.info("Dependencies " + dependencies.mkString(", "))
        dependencies.filter(_.data.getName.contains("-sub-project_")).foreach {
          moduleJar =>
            out.log.info(" module jar -> " + moduleJar.data)
            IO.unzip(moduleJar.data, assemblyJars)
        }
        out.log.info("Prepare manifest ...")
        IO.delete(manifest_mf)
        val sources = allFiles(assemblyJars).map {
          f => (f, relPath(f))
        }
        def manifest(mainClass: String, classPath: String) =
          new java.util.jar.Manifest(
            new ByteArrayInputStream(
              "Manifest-Version: 1.0\nClass-Path: %s\nMain-Class: %s\n".format(classPath, mainClass).getBytes
            )
          )
        out.log.info("Filter dependencies...")
        val filteredDependencies = dependencies.filter {
          _ match {
            case dep if dep.data.getName.contains("-sub-project_") => false
            case dep if dep.data.getName.contains("guice-all-2.0") => false
            case _ => true
          }
        }
        out.log.info("Filtered dependencies " + filteredDependencies.mkString(", "))
        filteredDependencies.foreach {
          depJar =>
            out.log.info(" dependency jar -> " + depJar.data.getName)
            IO.copyFile(depJar.data, target / "deps" / depJar.data.getName,
              preserveLastModified = false)
        }
        val manifestClassPath = filteredDependencies.map {
          _.data.getName
        }.foldLeft("") {
          _ + "\n  deps/" + _
        }
        Package.makeJar(sources, target / "carbay.jar",
          manifest(main.getOrElse(""), manifestClassPath), out.log)
        IO.delete(assemblyJars)
    }
}








