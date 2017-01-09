package io.deepsense.sdk.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.reflect.runtime.{universe => ru}

import io.deepsense.deeplang._
import io.deepsense.deeplang.doperables.dataframe.DataFrame
import io.deepsense.deeplang.inference.{InferContext, InferenceWarnings}
import io.deepsense.deeplang.params._
import io.deepsense.deeplang.params.validators.RangeValidator
import io.deepsense.deeplang.refl.Register

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