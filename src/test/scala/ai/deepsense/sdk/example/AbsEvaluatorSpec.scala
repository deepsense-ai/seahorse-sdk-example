/**
 * Copyright 2018 deepsense.ai (CodiLime, Inc)
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

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.Row
import org.scalatest._
import ai.deepsense.deeplang.doperables.dataframe.DataFrame
import ai.deepsense.deeplang.doperables.spark.wrappers.evaluators.RegressionEvaluator
import ai.deepsense.deeplang.params.ParamPair
import ai.deepsense.deeplang.params.selections.NameSingleColumnSelection
import ai.deepsense.sdk.example.HelperMock

class AbsEvaluatorSpec extends WordSpec with Matchers {
  "AbsEvaluator" should {
    "return abs metric" in {
      val evaluator = new AbsEvaluator()
      val df = {
        val schema = StructType(Seq(StructField("col1", DoubleType)))
        val rows = Seq(1.0, 0.0, -2.0).map(Row(_))
        DataFrame.fromSparkDataFrame(
          HelperMock.sparkSession.createDataFrame(HelperMock.sparkContext.parallelize(rows), schema))
      }
      evaluator.setPredictionColumn(NameSingleColumnSelection("col1"))

      val result = evaluator._evaluate(HelperMock.executionContext, df)
      result.value shouldBe 3.0
    }
  }
}
