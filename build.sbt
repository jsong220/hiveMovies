
// The simplest possible sbt build file is just one line:

scalaVersion := "2.13.3"
// That is, to create a valid sbt build, all you've got to do is define the
// version of Scala you'd like your project to use.

// ============================================================================

// Lines like the above defining `scalaVersion` are called "settings". Settings
// are key/value pairs. In the case of `scalaVersion`, the key is "scalaVersion"
// and the value is "2.13.3"

// It's possible to define many kinds of settings, such as:

name := "hello-world"
organization := "ch.epfl.scala"
version := "1.0"

// Note, it's not required for you to define these three settings. These are
// mostly only necessary if you intend to publish your library's binaries on a
// place like Sonatype or Bintray.


// Want to use a published library in your project?
// You can define other libraries as dependencies in your build like this:

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
// https://mvnrepository.com/artifact/org.apache.hive/hive-jdbc
libraryDependencies += "org.apache.hive" % "hive-jdbc" % "3.1.2"

