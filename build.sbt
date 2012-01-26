name := "silk"

organization := "org.xerial.silk"

version := "0.1-SNAPSHOT"

scalaVersion := "2.9.1"


//resolvers += "Sbt IDEA repo" at "http://mpeltonen.github.com/maven"

//resolvers += Classpaths.typesafeResolver

//addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "0.11.1-SNAPSHOT")


pomExtra :=
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
<url>http://xerial.org/</url>
<properties>
  <scala.version>2.9.1</scala.version>
  <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
</properties>
