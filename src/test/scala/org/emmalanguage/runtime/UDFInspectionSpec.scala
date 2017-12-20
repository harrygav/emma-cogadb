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
package runtime

import api._
import compiler.BaseCompilerSpec
import org.emmalanguage.compiler.CoGaDBCompiler
import org.emmalanguage.compiler.RuntimeCompiler


class UDFInspectionSpec extends BaseCompilerSpec with CoGaDBSpec {

  override val compiler = new RuntimeCompiler with CoGaDBCompiler

  import compiler._
  import u.reify

  lazy val testPipeline: u.Expr[Any] => u.Tree =
    pipeline(true)(Core.anf, collectFirstLambda, CoGaUDFSupport.specializeLambda
    ).compose(_.tree)

  lazy val testPipeline2: u.Expr[Any] => u.Tree =
    pipeline(true)(Core.anf, CoGaUDFSupport.specializeOps
    ).compose(_.tree)

  lazy val dscfPipeline: u.Expr[Any] => u.Tree =
    pipeline(true)(Core.dscf).compose(_.tree)

  lazy val anfPipeline: u.Expr[Any] => u.Tree =
    pipeline(true)(Core.anf, collectFirstLambda).compose(_.tree)

  lazy val collectFirstLambda: u.Tree => u.Tree = tree => (tree collect {
    case t: u.Function => t
  }).head


  "simple map udf" in withCoGaDB { implicit cogadb: CoGaDB =>

    val xsSeq = Seq(1 -> 2, 2 -> 2, 3 -> 4)
    val xs = new CoGaDBTable[(Int,Int)](cogadb.importSeq(xsSeq))

    val act = testPipeline2(reify {
      val zs = for {
        x <- xs
      } yield (x._1*18)
    })


  }
}
