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

import sbt._
import sbt.Keys._

/**
 * Release command.
 *
 * @author leo
 */
object Release {

  val releaseDirectory = SettingKey[File]("release-directory")
  lazy val settings: Seq[Setting[_]] = commandSettings ++ Seq[Setting[_]](
    releaseDirectory <<= crossTarget / "release"
  )

  lazy val commandSettings = Seq(
    commands += buildReleaseCommand
  )

  def buildReleaseCommand = Command.command("build-release") { state =>
      val extracted = Project.extract(state)
      val release = extracted.get(releaseDirectory)
      val releaseVersion = extracted.get(version)
      val projectRef = extracted.get(thisProjectRef)
      state
  }

}

