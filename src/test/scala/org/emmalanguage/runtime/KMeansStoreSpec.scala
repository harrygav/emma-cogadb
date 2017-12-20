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
import compiler.udf.UDFTransformer
import compiler.udf.common.MapUDFClosure
import io.csv.CSV
import io.csv.CSVScalaSupport

import org.scalatest._

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

class KMeansStoreSpec extends FreeSpec with Matchers with CoGaDBSpec {

  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  private def typecheck(ast: Tree): Tree = tb.typecheck(ast)

  val csv = CSV(delimiter = '|', quote = Some(' '), skipRows = 1)

  "k means example" in withCoGaDB { implicit cogadb: CoGaDB =>

    val t0 = System.nanoTime()

    val A = CSVScalaSupport[(Double, Double)](csv).read(getClass.getResource("/kmeans_points.csv").getPath).toStream
    val B = CSVScalaSupport[(Int, Double, Double)](csv)
      .read(getClass.getResource("/kmeans_centroids.csv").getPath).toStream


    val points = new CoGaDBTable[(Double, Double)](cogadb.importSeq(A))
    var centroids = new CoGaDBTable[(Int, Double, Double)](cogadb.importSeq(B))

    var mapCounter = 1;
    var computedChange = 1.0
    var iterations = 0;

    while (computedChange > 0.01) {
      println("CHANGE:" + computedChange + "CONTINUE:" + (computedChange > 0.01))

      //cross join of points and centroids
      val crossed = new CoGaDBTable[(Double, Double, Int, Double, Double)]({
        cogadb.saveSeq(ast.CrossJoin(points.rep, centroids.rep))
      })

      //apply map UDF on every (point, centroid) tuple to find distance
      val distance = typecheck(reify {
        () =>
          (pcs: (Double, Double, Int, Double, Double)) =>
            scala.math.sqrt(scala.math.pow(pcs._1 - pcs._4, 2) + scala.math.pow(pcs._2 - pcs._5, 2))
      }.tree)

      val map = new CoGaDBTable[(Double, Double, Int, Double, Double, Double)](new UDFTransformer(
        MapUDFClosure(distance, Map[String, String]("pcs" -> crossed.refTable), crossed.rep)).transform
      )
      mapCounter += 1

      //hand craft Reduce UDF for new centers
      val reduced = new CoGaDBTable[(Double, Double, Int)](
        cogadb.saveSeq(
          ast.GroupBy(Seq(crossed.ref("_1"), crossed.ref("_2")),
            Seq(
              ast.AggFuncReduce(
                ast.AlgebraicReduceUdf(
                  Seq(ast.ReduceUdfPayAttrRef("DOUBLE", "MIN_VALUE", ast.DoubleConst(4000000000D)),
                    ast.ReduceUdfPayAttrRef("OID", "ID", ast.IntConst(0))),
                  Seq(ast.ReduceUdfOutAttr("OID", "CID", "CID")),
                  Seq(ast.ReduceUdfCode("if(#<hash_entry>.MIN_VALUE#>computed_var_MAP_UDF_RES_" + (mapCounter - 1) +
                    "){#<hash_entry>.MIN_VALUE#=computed_var_MAP_UDF_RES_" + (mapCounter - 1) + ";" +
                    "#<hash_entry>.ID#=#" + map.refTable() + "._3#;" + "}")),
                  Seq(ast.ReduceUdfCode("#<out>.CID# = #<hash_entry>.ID#;"))
                )
              )
            ), map.rep
          )
        )
      )

      //val reduced = new CoGaDBTable[(Double, Double, Int)](cogadb.importSeq(reduce))

      //compute new cluster centers

      val newCentersRep = new CoGaDBTable[(Int, Double, Double)](
        cogadb.saveSeq(
          ast.GroupBy(
            Seq(
              reduced.ref("CID")
            ),
            Seq(ast.AggFuncSimple("AVG", reduced.ref("_1"), "_1"),
              ast.AggFuncSimple("AVG", reduced.ref("_2"), "_2")),
            reduced.rep
          )
        )
      )

      //val newCentersRep = new CoGaDBTable[(Int, Double, Double)](cogadb.importSeq(newCenters))

      //join old with new centroids

      val joinedCentroidsRep = new CoGaDBTable[(Int, Double, Double, Double, Double)](
        cogadb.saveSeq(
          ast.Projection(
            Seq(centroids.ref("_1"), centroids.ref("_2"), centroids.ref("_3"),
              newCentersRep.ref("_2"), newCentersRep.ref("_3")),
            ast.Join("INNER_JOIN",
              Seq(ast.ColCol(centroids.ref("_1"), newCentersRep.ref("_1"), ast.Equal)),
              centroids.rep, newCentersRep.rep
            ))
        )
      )

      //val joinedCentroidsRep = new CoGaDBTable[(Int, Double, Double, Double, Double)](cogadb.importSeq(joinedCentroids))

      //compute change
      val distance2 = typecheck(reify {
        () =>
          (xs: (Double, Double, Int, Double, Double)) =>
            scala.math.sqrt(scala.math.pow(xs._2 - xs._4, 2) + scala.math.pow(xs._3 - xs._5, 2))
      }.tree)

      val mapped2Rep = new CoGaDBTable[(Int, Double, Double, Double, Double, Double)](
        cogadb.saveSeq(
          new UDFTransformer(
            MapUDFClosure(distance2, Map[String, String]("xs" -> joinedCentroidsRep.refTable),
              joinedCentroidsRep.rep)).transform
        )
      )

      //val mapped2Rep = new CoGaDBTable[(Int, Double, Double, Double, Double, Double)](cogadb.importSeq(mapped2))


      val change = new CoGaDBTable[Double](

        ast.GroupBy(
          Seq(),
          Seq(ast.AggFuncSimple("SUM", mapped2Rep.ref("_6"), "_6")),
          mapped2Rep.rep
        )
      )

      computedChange = change.collect().head

      val projectedCentroids = new CoGaDBTable[(Int, Double, Double)](
        cogadb.saveSeq(
          ast.Projection(
            Seq(mapped2Rep.ref("_1"), mapped2Rep.ref("_4"), mapped2Rep.ref("_5")),
            mapped2Rep.rep
          )
        )
      )

      centroids = newCentersRep
      mapCounter += 1
      iterations += 1
    }

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) * Math.pow(10, -6) + "ms")
    println("Finished in " + iterations + " iterations with following centroids:")
    centroids.collect().foreach(println)


    //act.collect() should contain theSameElementsAs (exp)

  }

}
