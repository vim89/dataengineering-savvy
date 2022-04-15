name := "dataengineering-savvy"

version := "0.1"

scalaVersion := "2.12.15"

idePackagePrefix := Some("com.vitthalmirji.dataengineering")

val scalaCompactVersion = "2.12"
val scalaTestVersion = "3.2.11"
val sparkVersion = "2.4.8"
val jacksonVersion = "2.13.2"
val gcsVersion = "2.6.0"
val javaxMailVersion = "1.6.2"
val log4jVersion = "2.17.2"

// additional libraries
libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % scalaVersion.toString() % Provided,
  "org.scala-lang" % "scala-library" % scalaVersion.toString() % Provided,
  "org.scala-lang" % "scala-reflect" % scalaVersion.toString() % Provided,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
  "com.google.cloud" % "google-cloud-storage" % gcsVersion,
  "javax.mail" % "javax.mail-api" % javaxMailVersion
)

unmanagedBase := baseDirectory.value / "lib"

assemblyMergeStrategy in assembly := {
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps@_*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", xs@_*) =>
    xs map {
      _.toLowerCase
    } match {
      case "manifest.mf" :: Nil | "index.list" :: Nil | "dependencies" :: Nil =>
        MergeStrategy.discard
      case ps@x :: xs if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case "spring.schemas" :: Nil | "spring.handlers" :: Nil =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}