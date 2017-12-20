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
package compiler.lang.cogadb

import compiler.BaseCompilerSpec
import compiler.CoGaDBCompiler
import compiler.RuntimeCompiler
import org.emmalanguage.api.CoGaDBTable
import org.emmalanguage.api.cogadb.CoGaDBExp
import org.emmalanguage.api.cogadb.CoGaDBNtv
import org.emmalanguage.api.cogadb.CoGaDBOps

class ProjectionUDFSpec extends BaseCompilerSpec with CoGaDBSpec {

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


  "simple projection" in withCoGaDB(implicit cogadb => {
    val act = testPipeline(reify {
      (x: (Int, Double, String)) => {
        val u: Int = x._1
        u
      }
    })

    val exp = anfPipeline(reify {
      (x: ast.Row) => {
        val u = CoGaDBExp.proj(x, "_1")
        u
      }
    })

    act shouldBe alphaEqTo(exp)
  })

  "complex projection" in withCoGaDB(implicit cogadb => {
    val act = testPipeline(reify {
      (x: (Int, (Int, Int), String)) => {
        val u = x._2
        val v = u._1
        v

      }
    })

    val exp = anfPipeline(reify {
      (x: ast.Row) => {
        val u = CoGaDBExp.proj(x, "_2")
        val v = CoGaDBExp.proj(u, "_1")
        v
      }
    })

    act shouldBe alphaEqTo(exp)
  })

  "simple filter" in withCoGaDB(implicit cogadb => {
    val act = testPipeline(reify {
      (x: (Int, Int)) => {
        val v = x._1
        val r = v > 5
        r
      }
    })

    val exp = anfPipeline(reify {
      (x: ast.Row) => {
        val f1 = CoGaDBExp.proj(x, "_1")
        val f2 = CoGaDBExp.gt(f1, 5)
        f2
      }
    })
    act shouldBe alphaEqTo(exp)
  })

  "filter" in withCoGaDB(implicit cogadb => {
    val act = testPipeline(reify {
      (x: (Int, Int)) => {
        val v1 = x._1
        val u = v1 > 5
        val v = v1 < 3
        val r = u && v
        r
      }
    })

    val exp = anfPipeline(reify {
      (x: ast.Row) => {
        val v1 = CoGaDBExp.proj(x, "_1")
        val c1 = CoGaDBExp.gt(v1, 5)
        val b1 = CoGaDBExp.lt(v1, 3)
        val rs = CoGaDBExp.and(c1, b1)
        rs

      }
    })

    act shouldBe alphaEqTo(exp)
  })

  "join" in withCoGaDB(implicit cogadb => {
    val seq1 = Seq(1 -> 2, 2 -> 3, 3 -> 4)
    val seq2 = Seq(1 -> "foo", 2 -> "bar")

    val act = testPipeline2(reify {

      val s1 = CoGaDBTable(seq1)
      val s2 = CoGaDBTable(seq2)
      val f1 = (x: (Int, Int)) => {
        val y1 = x._1
        y1
      }
      val f2 = (x: (Int, String)) => {
        val z1 = x._1
        z1
      }

      val r1 = CoGaDBOps.equiJoin(f1, f2)(s1, s2)

      val f3 = (x: ((Int, Int), (Int, String))) => {
        val a1 = x._1
        val a2 = a1._1
        val b1 = x._2
        val b2 = b1._2
        val c = Tuple2.apply(a2, b2)
        c
      }

      val r2 = r1.map(f3)

      r2
    })

    val exp = dscfPipeline(reify {

      val s1 = CoGaDBTable(seq1)
      val s2 = CoGaDBTable(seq2)
      val f1 = (u: ast.Row) => {
        val y1 = CoGaDBExp.proj(u, "_1")
        y1
      }
      val f2 = (v: ast.Row) => {
        val y2 = CoGaDBExp.proj(v, "_1")
        y2
      }
      val x3 = CoGaDBNtv.equiJoin[(Int, Int), (Int, String), Int](f1, f2)(s1, s2)

      val f = (u: ast.Row) => {
        val y1 = CoGaDBExp.proj(u, "_1")
        val y11 = CoGaDBExp.proj(y1, "_1")
        val y2 = CoGaDBExp.proj(u, "_2")
        val y22 = CoGaDBExp.proj(y2, "_2")

        val r = CoGaDBExp.struct("_1", "_2")(y11, y22)
        r
      }

      val x5 = CoGaDBNtv.project[((Int, Int), (Int, String)), (Int, String)](f)(x3)
      x5
    })

    act shouldBe alphaEqTo(exp)
  }
  )
  "cross" in withCoGaDB(implicit cogadb => {
    val seq1 = Seq(1 -> 2, 2 -> 3, 3 -> 4)
    val seq2 = Seq(1 -> "foo", 2 -> "bar")

    val act = testPipeline2(reify {

      val s1 = CoGaDBTable(seq1)
      val s2 = CoGaDBTable(seq2)

      val r1 = CoGaDBNtv.cross(s1, s2)

      val f3 = (x: ((Int, Int), (Int, String))) => {
        val a1 = x._1
        val a2 = a1._1
        val b1 = x._2
        val b2 = b1._2
        val c = Tuple2.apply(a2, b2)
        c
      }


      val r2 = r1.map(f3)
      r2

    })

    val exp = dscfPipeline(reify {
      val s1 = CoGaDBTable(seq1)
      val s2 = CoGaDBTable(seq2)

      val x4 = CoGaDBNtv.cross(s1, s2)

      val f1 = (u: ast.Row) => {
        val y1 = CoGaDBExp.proj(u, "_1")
        val y11 = CoGaDBExp.proj(y1, "_1")
        val y2 = CoGaDBExp.proj(u, "_2")
        val y22 = CoGaDBExp.proj(y2, "_2")

        val r = CoGaDBExp.struct("_1", "_2")(y11, y22)
        r
      }

      val x5 = CoGaDBNtv.project[((Int, Int), (Int, String)), (Int, String)](f1)(x4)

      x5
    })

    println(act)
    println(exp)
    act shouldBe alphaEqTo(exp)


  })

  /*"filter test" ignore withCoGaDB(implicit cogadb => {
    val seq1 = Seq(1 -> 2, 2 -> 3, 3 -> 4)

    val act = testPipeline2(reify {

      val s1 = CoGaDBTable(seq1)

      val f3 = (x: (Int, Int)) => {
        val a1 = x._1
        val a2 = a1 * 3
        a2
      }

      val r2 = s1.map(f3)
      r2

    })

    val exp = dscfPipeline(reify {
      val s1 = CoGaDBTable(seq1)

      val f1 = (u: ast.Row) => {
        def timesThree(u: ast.Row,f: Int => Int) =
          def f(x: Int): Int = x*3
        val y1 = u.attr
        val y2 = y1
        y2
      }

      val x = CoGaDBNtv.project[(Int,Int),(Int,Int)](f1)(s1)

    })

    act shouldBe alphaEqTo(exp)
  })*/
}
