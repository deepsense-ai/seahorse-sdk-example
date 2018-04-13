/**
 * Copyright 2016 deepsense.ai (CodiLime, Inc)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

name := "seahorse-sdk-example"
version := "1.0"
scalaVersion := "2.11.8"

resolvers += Resolver.sonatypeRepo("public")
libraryDependencies += "ai.deepsense" %% "seahorse-executor-deeplang" % "1.4.3" % Provided

// TODO Get rid of this - find a way to do it properly in seahorse-workflow-executor
// These dependencies on Spark are necessary. As deeplang doesn't export its Spark dependencies, lack of them
// for this project will cause weird, at first glance unrelated, errors in test.
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.2" % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.2" % Provided
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.0.2" % Provided

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % Test

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)


