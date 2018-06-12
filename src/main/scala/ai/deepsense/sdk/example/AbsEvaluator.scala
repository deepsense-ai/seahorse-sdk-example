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

import ai.deepsense.deeplang.{DKnowledge, ExecutionContext}
import ai.deepsense.deeplang.doperables.dataframe.DataFrame
import ai.deepsense.deeplang.doperables.spark.wrappers.params.common.HasPredictionColumnSelectorParam
import ai.deepsense.deeplang.doperables.{Evaluator, MetricValue}
import ai.deepsense.deeplang.params.Param
import ai.deepsense.deeplang.params.selections.SingleColumnSelection
import breeze.numerics.abs
import org.apache.spark.sql.Row


final class AbsEvaluator extends Evaluator with HasPredictionColumnSelectorParam
{
  override val params: Array[Param[_]] = Array(predictionColumn)
  def setPredictionColumn(value: SingleColumnSelection): this.type =
    set(predictionColumn, value)

  val metricName = "Abs value"

  override def isLargerBetter: Boolean = true

  override def _evaluate(context: ExecutionContext, dataFrame: DataFrame): MetricValue = {
    val labelColumnName = dataFrame.getColumnName($(predictionColumn))
    val predictionDataframe = dataFrame.sparkDataFrame.select(labelColumnName)

    val sum = predictionDataframe.rdd.map { case Row(value: Double) => abs(value) }
    val metric = sum.reduce(_ + _)
    MetricValue(metricName, metric)
  }

  override def _infer(k: DKnowledge[DataFrame]): MetricValue = {
    MetricValue.forInference(metricName)
  }


}
