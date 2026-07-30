ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.8"

lazy val root = (project in file("."))
  .enablePlugins(GatlingPlugin) // adds the `Gatling` test config + `Gatling/test` task
  .settings(
    name := "scala-hand-on",

    // Run the app in its OWN JVM so JDK Mission Control sees a clean process
    // (not the sbt launcher), and so these JVM flags actually apply.
    run / fork := true,
    run / javaOptions ++= Seq(
      // Java Flight Recorder: continuous recording, NO fixed filename and NO JMX
      // port — so the third-party and the server JVMs can run at the SAME time
      // without fighting over port 7199 or one .jfr file. Dump a recording when
      // you want it via JMC (local attach → Dump) or:
      //   jcmd <pid> JFR.dump name=singleflight filename=target/<name>.jfr
      "-XX:StartFlightRecording=name=singleflight,settings=profile,maxsize=200m,maxage=30m",
      "-XX:FlightRecorderOptions=stackdepth=128" // deeper stacks for hot-method analysis
    )
  )

val AkkaVersion     = "2.8.8"
val AkkaHttpVersion = "10.5.3"
val GatlingVersion  = "3.15.1"
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed"          % AkkaVersion,
  "com.typesafe.akka" %% "akka-stream"               % AkkaVersion,     // akka-http is built on streams
  "com.typesafe.akka" %% "akka-http"                 % AkkaHttpVersion, // HTTP server + routing DSL
  "com.typesafe.akka" %% "akka-actor-testkit-typed"  % AkkaVersion % Test,
  // Gatling load testing. Simulations live in src/test/scala.
  "io.gatling.highcharts" % "gatling-charts-highcharts" % GatlingVersion % Test,
  "io.gatling"            % "gatling-test-framework"     % GatlingVersion % Test
)
