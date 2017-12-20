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

import api.CoGaDBTable
import compiler.lang.cogadb.ast
import compiler.udf.ReduceUDFGenerator
import io.csv.CSV
import io.csv.CSVScalaSupport

import org.scalatest._

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

class NaiveBayesSpec extends FreeSpec with Matchers with CoGaDBSpec {


  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  private def typecheck(ast: Tree): Tree = tb.typecheck(ast)

  val csv = CSV(delimiter = '|', quote = Some(' '), skipRows = 1)

  "naive bayes example" ignore withCoGaDB { implicit cogadb: CoGaDB =>
    val A = CSVScalaSupport[(Int, Int, Int)](csv).read(getClass.getResource("/bayes_prob.csv").getPath).toStream

    val data = new CoGaDBTable[(Int, Int, Int)](cogadb.importSeq(A))

    //calculate total rows
    val totalRows = new CoGaDBTable[Int](
      ast.GroupBy(
        Seq(),
        Seq(
          ast.AggFuncSimple("COUNT", data.ref("_3"), "_1")
        ),
        data.rep
      )
    ).collect()

    val totalCounts = totalRows.head

    println(totalCounts)

    //probability for each label
    val symbolTable = Map[String, String](
      "p" -> data.refTable())

    val z = typecheck(reify {
      1D / 10
    }.tree)

    val sngAst = typecheck(reify {
      () =>
        (p: (Int, Int, Int)) =>
          val ret = null
          ret
    }.tree)

    val uniAst = typecheck(reify {
      () =>
        (x: Double, y: Double) =>
          x + y
    }.tree)

    val labelCounts = new CoGaDBTable[(Int, Int)](
      ast.GroupBy(
        Seq(
          data.ref("_3")
        ),
        Seq(
          new ReduceUDFGenerator(z, sngAst, uniAst, symbolTable, data.rep).generate),
        data.rep
      )
    ).collect()


    labelCounts.foreach(println)
    /*

        //count per feature per label

        val featureLabelCounts = new CoGaDBTable[(Int, Int)](
          ast.GroupBy(
            Seq(
              data.ref("_3"),
              data.ref("_2")
            ),
            Seq(
              ast.AggFuncSimple("COUNT", data.ref("_3"), "_2")
            ),
            data.rep
          )
        ).collect()


        val totalCounts = counts.unzip match {
          case (l1, l2) => l2.sum
        }

        //iterate each class and calculate its probability
        val classProbs = List()
        for (curClass <- counts) {
          val classProb = (curClass._1, (curClass._2.toDouble / totalCounts))
          classProbs :+ classProb
        }

        classProbs.foreach(println)
    */


  }


}
