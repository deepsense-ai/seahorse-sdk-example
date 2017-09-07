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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.scalatest.{Matchers, WordSpec}

import ai.deepsense.deeplang.doperables.dataframe.DataFrame

class RandomSplitSpec  extends WordSpec with Matchers {
  "RandomSplit" should {
    "split DataFrame using given proportion" in {
      val operation = new RandomSplit().setSplitRatio(0.25)
      val elements = (1 to 100000).map(_.toDouble)
      val df = {
        val schema = StructType(Seq(StructField("col", DoubleType)))
        val rows = elements.map(Row(_))
        DataFrame.fromSparkDataFrame(
          HelperMock.sparkSession.createDataFrame(HelperMock.sparkContext.parallelize(rows), schema))
      }
      val Seq(left : DataFrame, right : DataFrame) = operation.executeUntyped(Vector(df))(HelperMock.executionContext)

      val delta = 1000 // 1% of 100000
      (left.sparkDataFrame.count() - 25000).abs.toInt should be < delta
      val leftAndRight = (left.sparkDataFrame.collect().toList ++ right.sparkDataFrame.collect().toList).map {
        case Row(x : Double) => x
      }
      leftAndRight.sorted shouldEqual elements.sorted
    }
  }
}
