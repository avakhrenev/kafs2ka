import java.nio.charset.StandardCharsets

import sbt.Keys.javaOptions

inThisBuild(
  Seq(
      scalaVersion := "2.12.2"
    , version := "0.0.1"
  )
)
val commonScalacOptions = Seq(
    "-encoding"
  , "UTF-8"
  , "-deprecation"
  , "-unchecked"
  , "-feature"
  , "-language:higherKinds"
  , "-language:existentials"
  , "-language:implicitConversions"
  , "-Ywarn-dead-code"
  , "-Ywarn-unused-import"
  , "-Xlint:unsound-match"
  , "-Ypatmat-exhaust-depth"
  , "40"
  , "-Ypartial-unification"
  , "-opt:l:method"
)

lazy val kafs2ka = (
  project in file(".")
  settings (
    name := "kafs2ka"
  , scalacOptions ++= commonScalacOptions
  , libraryDependencies ++= Seq(
      "co.fs2"           %% "fs2-core"       % "0.10.0-M4"
    , "co.fs2"           %% "fs2-io"         % "0.10.0-M4"
    , "org.apache.kafka" %% "kafka"          % "0.11.0.0"  % Test
    , "org.apache.kafka"  % "kafka-clients"  % "0.11.0.0"  % Test
    , "com.lihaoyi"      %% "utest"          % "0.4.8"     % Test
    )
  , fork in Test := true
  , javaOptions in Test ++= Seq(
    s"""-Dsbt.test.tempdir="${(target.value / "tmp").getPath}""""
  )
  , testFrameworks += new TestFramework("utest.runner.Framework")
  )
  settings(inConfig(Test)(Seq(
      javaOptions ++= Seq(
        s"""-Dsbt.test.tempdir="${(target.value / "tmp").getPath}""""
    )
  )):_*)
)
