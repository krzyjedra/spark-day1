ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.8"

lazy val root = (project in file("."))
  .settings(
    name := "spark-day1"
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"
