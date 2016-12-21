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

import scala.reflect.runtime.{universe => ru}

import io.deepsense.deeplang.DOperation.Id
import io.deepsense.deeplang._
import io.deepsense.deeplang.doperables.dataframe.DataFrame
import io.deepsense.deeplang.refl.Register

@Register
final class IdentityOperation
  extends DOperation1To1[DataFrame, DataFrame] {
  override val id: Id = "e9990168-daf7-44c6-8e0c-fbc50456fbec"
  override val name: String = "Identity"
  override val description: String = "Doesn't do anything"

  override protected def execute(input: DataFrame)(context: ExecutionContext): DataFrame = input
  override def params = Array.empty
  override def tTagTI_0: ru.TypeTag[DataFrame] = implicitly
  override def tTagTO_0: ru.TypeTag[DataFrame] = implicitly
}
