/*
 * Copyright 2018 Astraea, Inc.
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


import ai.deepsense.deeplang.DOperation.Id
import ai.deepsense.deeplang.doperables.dataframe.DataFrame
import ai.deepsense.deeplang.params.validators.RangeValidator
import ai.deepsense.deeplang.params.{BooleanParam, NumericParam, Param, Params}
import ai.deepsense.deeplang.{DOperation0To1, ExecutionContext}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import scala.reflect.runtime.universe._

import ai.deepsense.deeplang.doperables.DatetimeDecomposer.TimestampPart
import ai.deepsense.deeplang.doperables.spark.wrappers.params.common.OptionalQuantilesColumnChoice
import ai.deepsense.deeplang.params.choice.{Choice, ChoiceParam, MultipleChoiceParam}
import ai.deepsense.deeplang.params.wrappers.spark.SingleColumnCreatorParamWrapper

/**
  * Serves as a source of random vector data. Also exemplifies use of SPI for registering
  * new operations.
  */
class RandomVector extends DOperation0To1[DataFrame] {
  override def tTagTO_0: TypeTag[DataFrame] = implicitly
  override val id: Id = "c00bcfe4-c13b-40af-ae4d-bacfe0f1f73a"
  override val name: String = "Random Vector DataFrame"
  override val description: String = "Generates a DataFrame containing random vectors."
  override def specificParams: Array[Param[_]] = Array(vectorLength, numRows, shouldBias)

  val vectorLength = NumericParam(
    name = "vector length",
    description = Some("Number of elements in the randomly generated vectors"),
    validator = RangeValidator(1.0, 1000.0, step = Some(1.0))
  )
  setDefault(vectorLength, 10.0)
  def getVectorLength: Int = $(vectorLength).toInt
  def setVectorLength(len: Int): this.type = set(vectorLength, len.toDouble)

  val numRows = NumericParam(
    name = "row count",
    description = Some("Number of rows in the generated DataFrame"),
    validator = RangeValidator.positiveIntegers
  )
  setDefault(numRows, 1000.0)
  def getNumRows: Int = $(numRows).toInt
  def setNumRows(count: Int): this.type = set(numRows, count.toDouble)

  val shouldBias = ChoiceParam[BiasPart](
    name = "should bias",
    description = Some("Should numbers be biased in some direction")
  )
  setDefault(shouldBias, NoBiasNumber())

  override protected def execute()(context: ExecutionContext): DataFrame = {
    val len = getVectorLength
    val count = getNumRows
    val session = context.sparkSQLSession.getSparkSession
    import session.implicits._
    val rdd = session.sparkContext.parallelize(0 until count).mapPartitions { indexes =>
      val rnd = new scala.util.Random()
      for(_ <- indexes) yield {
        val values = Array.fill(len)(rnd.nextGaussian())
        Vectors.dense(values)
      }
    }

    implicit val vecEnc = ExpressionEncoder[Vector]
    val df = rdd.toDF("vectors")

    DataFrame.fromSparkDataFrame(df)
  }
}


sealed trait BiasPart extends Choice {
  override val choiceOrder: List[Class[_ <: Choice]] = List(classOf[NoBiasNumber], classOf[BiasNumber])
}
case class BiasNumber() extends BiasPart {
  override val name: String = "number"
  val number = NumericParam(name = "number", description = None)
  override val params: Array[Param[_]] = Array(number)
}

case class NoBiasNumber() extends BiasPart {
  override val name: String = "no"
  override val params: Array[Param[_]] = Array()
}