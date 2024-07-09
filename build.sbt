ThisBuild / scalaVersion := "2.12.15"

lazy val sparkVersion = "3.3.2"

lazy val root = (project in file("."))
  .settings(
    name := "Spark",
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.5"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.31.0",
  "com.google.cloud" % "google-cloud-bigquery" % "2.23.2",
  "com.mysql" % "mysql-connector-j" % "8.0.32",
  "org.postgresql" % "postgresql" % "42.5.4",
  "com.typesafe.play" %% "play-json" % "2.9.4",
  "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop3-2.2.16",
  "io.circe" %% "circe-core" % "0.14.5",
  "io.circe" %% "circe-parser" % "0.14.5",
  "com.github.scopt" %% "scopt" % "4.1.0",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.5",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.5",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

ThisBuild / assemblyShadeRules := Seq(
  ShadeRule.rename("com.google.**" -> "shadeio.@1").inAll
)

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

ThisBuild / assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}_${sparkVersion}_${version.value}.jar"