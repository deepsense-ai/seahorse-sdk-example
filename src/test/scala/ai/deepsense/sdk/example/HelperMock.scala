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

package ai.deepsense.sdk.example

import java.util.UUID

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, DataFrame => SparkDataFrame}
import spray.json.JsObject

import ai.deepsense.api.datasourcemanager.model.Datasource
import ai.deepsense.commons.models.Id
import ai.deepsense.commons.rest.client.datasources.{DatasourceClient, DatasourceInMemoryClient, DatasourceInMemoryClientFactory}
import ai.deepsense.deeplang._
import ai.deepsense.deeplang.doperables.dataframe.{DataFrame, DataFrameBuilder}
import ai.deepsense.deeplang.inference.InferContext
import ai.deepsense.deeplang.params.custom.InnerWorkflow
import ai.deepsense.sparkutils.SparkSQLSession

object HelperMock {
  private lazy val sparkConf: SparkConf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("TestApp")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .registerKryoClasses(Array())

  lazy val sparkContext = SparkContext.getOrCreate(sparkConf)

  lazy val sparkSession = (new SparkSession.Builder).config(sparkConf).getOrCreate()

  lazy val executionContext: ExecutionContext = {
    val sparkContext = SparkContext.getOrCreate()
    val sparkSession = new SparkSQLSession(sparkContext)
    val catalog = CatalogRecorder.resourcesCatalogRecorder.catalogs
    val innerWorkflowExecutor = new InnerWorkflowExecutor {
      override def execute(
          executionContext: CommonExecutionContext,
          workflow: InnerWorkflow,
          dataFrame: DataFrame): DataFrame = ???
      override def parse(workflow: JsObject): InnerWorkflow = ???
      override def toJson(innerWorkflow: InnerWorkflow): JsObject = ???
    }
    val dataFrameStorage = new DataFrameStorage {
      override def getInputDataFrame(workflowId: Id, nodeId: Id, portNumber: Int):
        Option[SparkDataFrame] = ???

      override def setInputDataFrame(workflowId: Id,
          nodeId: Id,
          portNumber: Int,
          dataFrame: SparkDataFrame): Unit = ???

      override def removeNodeInputDataFrames(workflowId: Id, nodeId: Id, portNumber: Int): Unit =
        ???

      override def removeNodeInputDataFrames(workflowId: Id, nodeId: Id): Unit = ???

      override def getOutputDataFrame(workflowId: Id, nodeId: Id, portNumber: Int):
      Option[SparkDataFrame] = ???

      override def setOutputDataFrame(workflowId: Id, nodeId: Id, portNumber: Int, dataFrame:
      SparkDataFrame): Unit = ???

      override def removeNodeOutputDataFrames(workflowId: Id, nodeId: Id): Unit = ???
    }
    val codeExecutor: CustomCodeExecutor = new CustomCodeExecutor {
      override def run(workflowId: String, nodeId: String, code: String): Unit = ???
      override def isValid(code: String): Boolean = ???
    }

    val inferContext = InferContext(DataFrameBuilder(sparkSession),
      catalog,
      new DatasourceInMemoryClient(Nil))

    val codeExecutionProvider =
      CustomCodeExecutionProvider(codeExecutor, codeExecutor, new OperationExecutionDispatcher)

    ExecutionContext(
      sparkContext,
      sparkSession,
      inferContext,
      ExecutionMode.Batch,
      LocalFileSystemClient(),
      "/tmp",
      "foo",
      innerWorkflowExecutor,
      ContextualDataFrameStorage(
        dataFrameStorage,
        "b3eae503-c9a1-442c-9a78-992617cd939e",
        "fc84c1dd-ba6b-4799-b68e-1e5d06e8007b"),
      None,
      None,
      ContextualCustomCodeExecutor(codeExecutionProvider,
        "b3eae503-c9a1-442c-9a78-992617cd939e",
        "fc84c1dd-ba6b-4799-b68e-1e5d06e8007b")
    )
  }
}
