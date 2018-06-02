/**
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

import ai.deepsense.deeplang.doperables.dataframe.DataFrame
import org.scalatest.{Matchers, WordSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{Vectors, Vector}


class RandomVectorSpec extends WordSpec with Matchers {
  "RandomVector" should {
    "generate a DataFrame of prescribed dimensions" in {
      val spark = HelperMock.sparkSession
      import spark.implicits._
      val rows = 30
      val length = 12

      val operation = new RandomVector()

      operation.setNumRows(rows)
      operation.setVectorLength(length)

      val result = operation.executeUntyped(Vector())(HelperMock.executionContext)

      val vlen = udf((v: Vector) => v.size)
      result match {
        case Vector(df: DataFrame) =>
          assert(df.sparkDataFrame.count() === rows)
          val counts = df.sparkDataFrame.select(vlen($"vectors") as "len").agg(min($"len"), max($"len")).as[(Int, Int)]
          val (minC, maxC) = counts.first()
          assert(minC === length)
          assert(maxC === length)
        case _ => fail("Expected a single DataFrame result.")
      }
    }
  }
}
