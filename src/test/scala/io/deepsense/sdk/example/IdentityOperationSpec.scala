/**
 * Copyright 2016, deepsense.io
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

package io.deepsense.sdk.example

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.Row
import org.scalatest._

import io.deepsense.deeplang.doperables.dataframe.DataFrame

class IdentityOperationSpec extends WordSpec with Matchers {
  "IdentityOperation" should {
    "output DataFrame provided as input" in {
      val operation = new IdentityOperation()
      val df = {
        val schema = StructType(Seq(
          StructField("col1", DoubleType),
          StructField("col2", StringType),
          StructField("col3", DoubleType),
          StructField("col4", DoubleType)
        ))
        val rows = Seq(
          (3.5, "a", 1.5, 5.0),
          (3.6, "b", 1.6, 6.0),
          (3.7, "c", 1.7, 10.0),
          (4.6, "d", 1.6, 9.0),
          (4.5, "e", 1.5, 11.0)
        ).map(Row.fromTuple)
        DataFrame.fromSparkDataFrame(
          HelperMock.sparkSession.createDataFrame(HelperMock.sparkContext.parallelize(rows), schema))
      }
      val result = operation.executeUntyped(Vector(df))(HelperMock.executionContext).head.asInstanceOf[DataFrame]
      result shouldEqual df
    }
  }
}
