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

import ai.deepsense.deeplang.DOperationCategories.{Action, Filtering, UserDefined}
import ai.deepsense.deeplang.catalogs.SortPriority
import ai.deepsense.deeplang.catalogs.doperations.DOperationCategory
import ai.deepsense.deeplang.catalogs.spi.{CatalogRegistrant, CatalogRegistrar}

/**
  * Example of using catalog registration SPI to register a new category and operation.
  *
  */
class SPIRegistration extends CatalogRegistrant {
  override def register(registrar: CatalogRegistrar): Unit = {
    val prios = SortPriority(12345).inSequence(10)
    registrar.registerOperation(SPIRegistration.CustomCategory, () => new RandomVector(), prios.next)
    registrar.registerOperation(Filtering, () => new RandomSplit(), SortPriority.lowerBound.next(1))
    val sdkDefault = SortPriority.sdkDefault
    registrar.registerOperation(UserDefined, () => new CreateAbsEvaluator(), sdkDefault.nextCore())
    registrar.registerOperable[AbsEvaluator]()
  }
}

object SPIRegistration {
  object CustomCategory extends DOperationCategory(
    "5e49fb33-ddaf-45a3-8d41-ef15b65200fe", "Custom SPI Category", Action.priority.next(1))
}
