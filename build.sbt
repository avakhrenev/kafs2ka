(watchTriggeredMessage in Global) := Watch.clearScreenOnTrigger

inThisBuild(
    Seq(
      scalaVersion := "2.12.8"
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
          "co.fs2"           %% "fs2-core"     % "1.0.5"
        , "co.fs2"           %% "fs2-io"       % "1.0.5"
        , "io.estatico"      %% "newtype"      % "0.4.2"
        , "org.apache.kafka" % "kafka-clients" % "2.1.1" % Test
        , "org.apache.kafka" %% "kafka"        % "2.1.1" % Test
        , "com.lihaoyi"      %% "utest"        % "0.7.1" % Test
      ) ++ Seq(
          "org.scalamacros" % "paradise"            % "2.1.1" cross CrossVersion.full
        , "org.typelevel"   %% "kind-projector"     % "0.10.0"
        , "com.olegpy"      %% "better-monadic-for" % "0.3.0"
      ).map(compilerPlugin)
    , fork in Test := true
    , javaOptions in Test ++= Seq(
          s"""-Dsbt.test.tempdir="${(target.value / "tmp").getPath}""""
      )
    , testFrameworks += new TestFramework("utest.runner.Framework")
  )
    settings (inConfig(Test)(
      Seq(
          javaOptions ++= Seq(
            s"""-Dsbt.test.tempdir="${(target.value / "tmp").getPath}""""
        )
      )): _*)
)
