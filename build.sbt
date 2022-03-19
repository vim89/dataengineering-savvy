name := "dataengineering-savvy"

version := "0.1"

scalaVersion := "2.12.15"

idePackagePrefix := Some("com.vitthalmirji.dataengineering")

val jarProvidedByEndUser = "provided"
val scalaCompactVersion = "2.12"
val sparkVersion = "2.4.8"
val jacksonVersion = "2.13.2"
val gcsVersion = "2.4.5"
val javaxMailVersion = "1.6.2"


// additional libraries
libraryDependencies ++= Seq(
  "org.scala-lang" %% "scala-compiler" % scalaVersion.toString() % jarProvidedByEndUser,
  "org.scala-lang" %% "scala-library" % scalaVersion.toString() % jarProvidedByEndUser,
  "org.scala-lang" %% "scala-reflect" % scalaVersion.toString() % jarProvidedByEndUser,
  "org.apache.spark" %% "spark-core" % sparkVersion % jarProvidedByEndUser,
  "org.apache.spark" %% "spark-sql" % sparkVersion % jarProvidedByEndUser,
  "org.apache.spark" %% "spark-hive" % sparkVersion % jarProvidedByEndUser,
  "com.fasterxml.jackson.core" %% "jackson-databind" % jacksonVersion % jarProvidedByEndUser,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion % jarProvidedByEndUser,
  "com.google.cloud" % "google-cloud-storage" % gcsVersion % jarProvidedByEndUser,
  "javax.mail" % "javax.mail-api" % javaxMailVersion % jarProvidedByEndUser
)
