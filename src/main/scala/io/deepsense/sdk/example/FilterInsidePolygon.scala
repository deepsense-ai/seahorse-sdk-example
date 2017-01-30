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

import com.esri.core.geometry.{GeoJsonImportFlags, Geometry, GeometryEngine, SpatialReference}
import io.deepsense.deeplang.DOperation.Id
import io.deepsense.deeplang._
import io.deepsense.deeplang.doperables.dataframe.{DataFrame, DataFrameColumnsGetter}
import io.deepsense.deeplang.inference.{InferContext, InferenceWarnings}
import io.deepsense.deeplang.params.{SingleColumnSelectorParam, StringParam}
import io.deepsense.deeplang.refl.Register

import scala.reflect.runtime.{universe => ru}
import scala.util.control.NonFatal

@Register
final class FilterInsidePolygon
  extends DOperation1To1[DataFrame, DataFrame] {
  override val id: Id = "48fa3638-bc8d-4430-909f-85d4ece824a3"
  override val name: String = "Filter Location Inside Polygon"
  override val description = "Doesn't do anything"

  lazy val geoJsonPointColumnSelector = SingleColumnSelectorParam(
    name = "GeoJson Point - Column Name",
    description = Some("Column name containing Geolocation written as Point type object in GeoJson."),
    portIndex = 0
  )

  lazy val geoJsonPolygon = StringParam(
    name = "GeoJson Polygon",
    description = Some("Polygon written in GeoJson format used for filtering out the rows.")
  )

  override def params = Array(geoJsonPointColumnSelector, geoJsonPolygon)

  override protected def execute(input: DataFrame)(context: ExecutionContext): DataFrame = {
    val polygon = GeometryEngine.geometryFromGeoJson(
      $(geoJsonPolygon),
      GeoJsonImportFlags.geoJsonImportDefaults,
      Geometry.Type.Polygon
    )
    val columnName = DataFrameColumnsGetter.getColumnName(
      input.schema.get,
      $(geoJsonPointColumnSelector)
    )

    val filtered = input.sparkDataFrame.filter(row => {
      try {
        val pointGeoJson = row.getAs[String](columnName)

        val point = GeometryEngine.geometryFromGeoJson(
          pointGeoJson,
          GeoJsonImportFlags.geoJsonImportDefaults,
          Geometry.Type.Point
        )

        val standardCoordinateFrameForEarth = SpatialReference.create(4326)

        GeometryEngine.contains(
          polygon.getGeometry,
          point.getGeometry,
          standardCoordinateFrameForEarth
        )
      } catch {
        case NonFatal(_) => false
      }
    })

    DataFrame.fromSparkDataFrame(filtered)
  }

  override protected def inferKnowledge(k0: DKnowledge[DataFrame])(context: InferContext): (DKnowledge[DataFrame], InferenceWarnings) = {
    GeometryEngine.geometryFromGeoJson(
      $(geoJsonPolygon),
      GeoJsonImportFlags.geoJsonImportDefaults,
      Geometry.Type.Polygon
    )

    super.inferKnowledge(k0)(context)
  }

  override def tTagTI_0: ru.TypeTag[DataFrame] = implicitly
  override def tTagTO_0: ru.TypeTag[DataFrame] = implicitly
}
