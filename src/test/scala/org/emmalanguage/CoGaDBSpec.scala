/*
 * Copyright © 2016 TU Berlin (emma@dima.tu-berlin.de)
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
package org.emmalanguage

import org.emmalanguage.api.DataBagEquality
import runtime.CoGaDB
import test.util._

import org.scalatest._

import java.io.File
import java.nio.file.Paths

trait CoGaDBSpec extends BeforeAndAfter with DataBagEquality with CoGaDBAware {
  this: Suite =>

  val resPath = "/cogadb"
  val tmpPath = tempPath(resPath)
  val configPath = Paths.get(materializeResource(s"$resPath/default.coga"))

  before {
    new File(tmpPath).mkdirs()
  }

  after {
    deleteRecursive(new File(tmpPath))
  }

  override def withCoGaDB[T](f: CoGaDB => T): T = {
    val cogadb = setupCoGaDB(CoGaDB.Config(configPath = configPath))
    val result = f(cogadb)
    destroyCoGaDB(cogadb)
    result
  }
}
