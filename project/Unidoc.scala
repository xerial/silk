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
import sbt._
import sbt.Keys._
import sbt.Project.Initialize

object Unidoc {
  val unidocDirectory = SettingKey[File]("unidoc-directory")
  val unidocExclude = SettingKey[Seq[String]]("unidoc-exclude")
  val unidocAllSources = TaskKey[Seq[Seq[File]]]("unidoc-all-sources")
  val unidocSources = TaskKey[Seq[File]]("unidoc-sources")
  val unidocAllClasspaths = TaskKey[Seq[Classpath]]("unidoc-all-classpaths")
  val unidocClasspath = TaskKey[Seq[File]]("unidoc-classpath")
  val unidoc = TaskKey[File]("unidoc", "Create unified scaladoc for all aggregates")

  lazy val settings = Seq(
    unidocDirectory <<= crossTarget / "unidoc",
    unidocExclude := Seq.empty,
    unidocAllSources <<= (thisProjectRef, buildStructure, unidocExclude)
    flatMap allSources,
    unidocSources <<= unidocAllSources map { _.flatten },
    unidocAllClasspaths <<= (thisProjectRef, buildStructure, unidocExclude)
    flatMap allClasspaths,
    unidocClasspath <<= unidocAllClasspaths map {
    _.flatten.map(_.data).distinct },
    unidoc <<= unidocTask
  )

  def allSources(projectRef: ProjectRef, structure: Load.BuildStructure,
  exclude: Seq[String]): Task[Seq[Seq[File]]] = {
    val projects = aggregated(projectRef, structure, exclude)
    projects flatMap { sources in Compile in LocalProject(_) get
  structure.data } join
  }

  def allClasspaths(projectRef: ProjectRef, structure: Load.BuildStructure,
  exclude: Seq[String]): Task[Seq[Classpath]] = {
    val projects = aggregated(projectRef, structure, exclude)
    projects flatMap { dependencyClasspath in Compile in LocalProject(_) get
  structure.data } join
  }

  def aggregated(projectRef: ProjectRef, structure: Load.BuildStructure,
  exclude: Seq[String]): Seq[String] = {
    val aggregate = Project.getProject(projectRef,
  structure).toSeq.flatMap(_.aggregate)
    aggregate flatMap { ref =>
      if (exclude contains ref.project) Seq.empty
      else ref.project +: aggregated(ref, structure, exclude)
    }
  }

  def unidocTask: Initialize[Task[File]] = {
    (compilers, cacheDirectory, unidocSources, unidocClasspath,
    unidocDirectory, scalacOptions in doc, streams) map {
      (compilers, cache, sources, classpath, target, options, s) => {
        val scaladoc = new Scaladoc(100, compilers.scalac)
        scaladoc.cached(cache / "unidoc", "main", sources, classpath, target,
    options, s.log)
        target
      }
    }
  }
}
