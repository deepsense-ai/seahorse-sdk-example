package io.deepsense.sdk.example

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.scalatest.{Matchers, WordSpec}

import io.deepsense.deeplang.doperables.dataframe.DataFrame

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