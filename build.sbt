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
    , "org.apache.kafka" %% "kafka"          % "0.11.0.0"  % Test exclude ("org.slf4j", "slf4j-log4j12")
    , "org.apache.kafka"  % "kafka-clients"  % "0.11.0.0"  % Test
    , "com.lihaoyi"      %% "utest"          % "0.4.8"     % Test
    //logging in tests
    , "org.slf4j"                % "slf4j-api"        % "1.7.22"    % Test
    , "org.slf4j"                % "jcl-over-slf4j"   % "1.7.22"    % Test
    , "org.apache.logging.log4j" % "log4j-jul"        % "2.7"       % Test
    , "org.apache.logging.log4j" % "log4j-api"        % "2.7"       % Test
    , "org.apache.logging.log4j" % "log4j-core"       % "2.7"       % Test
    , "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.7"       % Test
    )
  , fork in Test := true
  , javaOptions in Test ++= Seq(
    s"""-Dsbt.test.tempdir="${(target.value / "tmp").getPath}""""
  )
  , testFrameworks += new TestFramework("utest.runner.Framework")
  )
  settings(inConfig(Test)(Seq(
      resourceGenerators += generateLoggingConfigsTask(Test).taskValue
    , javaOptions ++= Seq(
        s"""-Dsbt.test.tempdir="${(target.value / "tmp").getPath}""""
      , "-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager"
    )
  )):_*)
)



def generateLoggingConfigsTask(cfg: Configuration): Def.Initialize[Task[Seq[File]]] = Def.task {
  generateLoggingConfigs(
    name.value
    , cfg
    , (target in cfg).value / "logs"
    , (resourceManaged in cfg).value
  )
}

def generateLoggingConfigs(projName: String,
                           config: Configuration,
                           logDir: File,
                           outputDir: File): Seq[File] = {
  val log4j1   = outputDir / "log4j.xml"
  val log4j2   = outputDir / "log4j2.xml"
  val logfile  = logDir / s"$projName-${config.name}.log"
  val logfile2 = logDir / s"$projName-2-${config.name}.log"
  IO.write(log4j1,
    s"""<?xml version="1.0" encoding="UTF-8" ?>
       |<!DOCTYPE log4j:configuration SYSTEM "log4j.dtd">
       |<log4j:configuration xmlns:log4j="http://jakarta.apache.org/log4j/">
       |  <appender name="file" class="org.apache.log4j.FileAppender">
       |    <param name="File" value=${escaped(logfile.getPath)}/>
       |    <param name="Append" value="false" />
       |    <layout class="org.apache.log4j.PatternLayout">
       |      <param name="ConversionPattern" value="%d{HH:mm:ss.SSS} [%-5p] %c{1} - %m%n"/>
       |    </layout>
       |  </appender>
       |  <root>
       |    <priority value="debug"/>
       |    <appender-ref ref="file"/>
       |  </root>
       |</log4j:configuration>""".stripMargin,
    StandardCharsets.UTF_8)
  IO.write(log4j2,
    s"""<?xml version="1.0" encoding="UTF-8"?>
       |<Configuration>
       |  <Appenders>
       |    <File name="File" fileName=${escaped(logfile2.getPath)} append="false">
       |      <PatternLayout pattern="%d{HH:mm:ss.SSS} [%-5p] %c{1} - %m%n"/>
       |    </File>
       |  </Appenders>
       |  <Loggers>
       |    <Logger name="org.apache" level="debug" additivity="false">
       |      <AppenderRef ref="File"/>
       |    </Logger>
       |    <Root level="info">
       |      <AppenderRef ref="File"/>
       |    </Root>
       |  </Loggers>
       |</Configuration>""".stripMargin,
    StandardCharsets.UTF_8)
  Seq(log4j1, log4j2)
}
//escaped representation of the string.
//http://stackoverflow.com/questions/9913971/scala-how-can-i-get-an-escaped-representation-of-a-string
def escaped(str: String): String = {
  import scala.reflect.runtime.universe._
  Literal(Constant(str)).toString
}
