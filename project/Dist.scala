/*
 * Copyright 2012 Taro L. Saito
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * @author leo
 */

import sbt._
import sbt.Keys._
import sbt.classpath.ClasspathUtilities

/**
 * Settings for generating distribution packages.
 */
object Dist {

  val distExclude = SettingKey[Seq[String]]("dist-exclude")
  val distAllClasspaths = TaskKey[Seq[Classpath]]("dist-all-classpaths")
  val distDependencies = TaskKey[Seq[File]]("dist-dependencies")
  val distLibJars = TaskKey[Seq[File]]("dist-lib-jars")

  lazy val settings: Seq[Setting[_]] = Seq(
    distExclude := Seq.empty,
    distAllClasspaths <<= (thisProjectRef, buildStructure, distExclude) flatMap getFromAllProjects(dependencyClasspath.task in Compile),
    distDependencies <<= distAllClasspaths.map {
      _.flatten.map(_.data).filter(ClasspathUtilities.isArchive).distinct
    },
    distLibJars <<= (thisProjectRef, buildStructure, distExclude) flatMap getFromAllProjects(packageBin.task in Compile)
  )

  def getFromAllProjects[T](targetTask: SettingKey[Task[T]])(currentProject: ProjectRef, structure: Load.BuildStructure, exclude:Seq[String]): Task[Seq[T]] = {
    val projects: Seq[ProjectRef] = childProjectRefs(currentProject, structure, exclude)
    projects.flatMap{ targetTask in _ get structure.data} join
  }

  def childProjectRefs(currentProject: ProjectRef, structure: Load.BuildStructure, exclude:Seq[String]): Seq[ProjectRef] = {
    val children = Project.getProject(currentProject, structure).toSeq.flatMap(_.aggregate)
    children flatMap {
      ref =>
        if(exclude.contains(ref.project))
          Seq.empty
        else
          ref +: childProjectRefs(ref, structure, exclude)
    }
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

  def packageDistTask: Setting[Task[File]] = packageDist <<= (update, version, distLibJars, distDependencies, streams, target, dependencyClasspath in Runtime, classDirectory in Compile, baseDirectory) map {
    (up, ver, libs, depJars, out, target, dependencies, classDirectory, base) => {

      val distDir = target / "dist"

      out.log.info("output dir: " + distDir)
      IO.delete(distDir)
      distDir.mkdirs()

      out.log.info("distLibJars:\n" + libs.mkString("\n"))
      out.log.info("distDependencies:\n" + depJars.mkString("\n"))

      out.log.info("Copy libraries")
      val libDir = distDir / "lib"
      libDir.mkdirs()
      (libs ++ depJars).foreach(l => IO.copyFile(l, libDir / l.getName))

      out.log.info("Create bin folder")
      val binDir = distDir / "bin"
      binDir.mkdirs()
      IO.copyDirectory(base / "src/script", binDir)
      // chmod +x
      if (!System.getProperty("os.name", "").contains("Windows")) {
        scala.sys.process.Process("chmod -R +x %s".format(binDir)).run
      }

      out.log.info("Generating version info")
      IO.write(distDir / "VERSION", ver + "\n")
      out.log.info("done.")

      distDir
    }
  }


}