name := "seahorse-sdk-example"
version := "1.0"
scalaVersion := "2.11.8"

// TODO Use stable version - this is snapshot from branch dev_sdk
resolvers += Resolver.sonatypeRepo("public")
libraryDependencies += "io.deepsense" %% "deepsense-seahorse-deeplang" % "1.3.0-LOCAL-SNAPSHOT" % Provided

// TODO Get rid of this - find a way to do it properly in seahorse-workflow-executor
// These dependencies on Spark are necessary. As deeplang doesn't export its Spark dependencies, lack of them
// for this project will cause weird, at first glance unrelated, errors in test.
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0" % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0" % Provided
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.0.0" % Provided

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % Test

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)


