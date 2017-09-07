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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.reflect.runtime.{universe => ru}

import ai.deepsense.deeplang._
import ai.deepsense.deeplang.doperables.dataframe.DataFrame
import ai.deepsense.deeplang.inference.{InferContext, InferenceWarnings}
import ai.deepsense.deeplang.params._
import ai.deepsense.deeplang.params.validators.RangeValidator
import ai.deepsense.deeplang.refl.Register

@Register
final class RandomSplit()
  extends DOperation1To2[DataFrame, DataFrame, DataFrame] with Params {
  override val id: DOperation.Id = "37648959-2424-4c50-b0af-f652565ebd91"
  override val name: String = "Random Split"
  override val description = "Splits a DataFrame into two DataFrames randomly."

  val splitRatio = NumericParam(
    name = "split ratio",
    description = Some("Percentage of rows that should end up in the first output DataFrame."),
    validator = RangeValidator(0.0, 1.0, beginIncluded = true, endIncluded = true))
  setDefault(splitRatio, 0.5)
  def setSplitRatio(value: Double): this.type = set(splitRatio, value)
  def getSplitRatio: Double = $(splitRatio)

  val seed = NumericParam(
    name = "seed",
    description = Some("Seed for pseudo random number generator."),
    validator = RangeValidator(Int.MinValue, Int.MaxValue, step = Some(1.0))
  )
  setDefault(seed, 0.0)
  def getSeed: Int = $(seed).toInt
  override val params: Array[Param[_]] = Array(splitRatio, seed)
  override protected def execute(
                                  df: DataFrame)(
                                  context: ExecutionContext): (DataFrame, DataFrame) = {
    val Array(f1: RDD[Row], f2: RDD[Row]) =
      df.sparkDataFrame.rdd.randomSplit(Array(getSplitRatio, 1.0 - getSplitRatio), getSeed)
    val schema = df.sparkDataFrame.schema
    val dataFrame1 = context.dataFrameBuilder.buildDataFrame(schema, f1)
    val dataFrame2 = context.dataFrameBuilder.buildDataFrame(schema, f2)
    (dataFrame1, dataFrame2)
  }
  override protected def inferKnowledge(knowledge: DKnowledge[DataFrame])(context: InferContext)
  : ((DKnowledge[DataFrame], DKnowledge[DataFrame]), InferenceWarnings) = {
    ((knowledge, knowledge), InferenceWarnings.empty)
  }
  override def tTagTI_0: ru.TypeTag[DataFrame] = implicitly
  override def tTagTO_0: ru.TypeTag[DataFrame] = implicitly
  override def tTagTO_1: ru.TypeTag[DataFrame] = implicitly
}
