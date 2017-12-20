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
package api

import api.alg.Alg
import compiler.lang.cogadb._
import compiler.lang.cogadb.ast.MapUdf
/*import org.emmalanguage.compiler.udf.UDFTransformer
import org.emmalanguage.compiler.udf.common.MapUDFClosure*/
import runtime.CoGaDB

import scala.language.implicitConversions
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox


/** A `DataBag` implementation backed by a CoGaDB `Table`. */
class CoGaDBTable[A: Meta] private[emmalanguage]
(
  private[emmalanguage] val rep: ast.Op
)(
  @transient implicit val cogadb: CoGaDB
) extends DataBag[A] {

  @transient override val m = implicitly[Meta[A]]


  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  private def typecheck(ast: Tree): Tree = tb.typecheck(ast)

  @transient val col: ast.Row = {
    println("REP " + rep)
    val t = m.ttag.tpe
    val u = t.typeArgs

    /*    println("TAG " + m.ttag.toString())
        val s = m.ctag
        println(s"PARAMS $u with length ${u.length} ")
        println("-----")

        //val keys = (1 to u.length).map(x => "_" + x).toList


        println("-----")*/

    /*def rowFor(tt: u.TypeTag): ast.Row = {
      ast.StructRow(Seq("_1","_2"),Seq(ast.SimpleRow(),ast.SimpleRow()))
    }

    m.ttag.tpe match {
      case TypeRef(_, _, args) =>
        println(s"cur args: $args")
        args.map {
          arg =>
            if (arg <:< typeOf[Product]) {
              println(s"$arg is a product and will be resolved")
              resolveTypes(arg)
              1
            }
            else {
              println(s"$arg is not a product");
              2
            }
        }
      case _ => throw new IllegalArgumentException("problem")
    }

    def resolveTypes[T](ttag: Type): Unit = {
      ttag match {
        case x if x <:< typeOf[Int] || x <:< typeOf[String] => {
          x match {
            case TypeRef(_, _, args) =>
              println(s"case 1 $args")
              args.map {
                arg =>
                  println(s"resolve: $arg is a product and will be resolved again");
                  resolveTypes(arg)
                  1
              }
          }

        }
        case _ => print(s"$ttag final")
      }
    }*/
    //println(s"new tag $tag")

    //u.map(x => println("symb" + x.termSymbol))

    def construct(curRep: ast.Node): Seq[String] = {
      curRep match {
        case ast.Join(_, _, lhs, rhs) => Seq()
        case ast.CrossJoin(lhs, rhs) => Seq()
        case _ => Seq()
      }
    }

    rep match {
      case ast.Join(predicate, _, lhs, rhs) =>

        val lkeys = (1 to u.head.typeArgs.length).map(x => "_" + x).toList
        val lvals = (1 to u.head.typeArgs.length).map(x =>
          ast.SimpleRow(ast.AttrRef(resolveTable()(lhs), "_" + x, "_" + x)))

        val rkeys = (1 to u.tail.head.typeArgs.length).map(x => "_" + x).toList
        val rvals = (1 to u.tail.head.typeArgs.length).map(x =>
          ast.SimpleRow(ast.AttrRef(resolveTable()(rhs), "_" + x, "_" + x)))

        ast.StructRow(
          Seq("_1", "_2"),
          Seq(
            ast.StructRow(lkeys, lvals),
            ast.StructRow(rkeys, rvals)
          ))
      case ast.CrossJoin(lhs, rhs) =>
        //hardcoded for two tables with two attributes
        ast.StructRow(Seq("_1", "_2"),
          Seq(
            ast.StructRow(
              Seq("_1", "_2"),
              Seq(
                ast.SimpleRow(ast.AttrRef("DATAFLOW0000", "_1", "_1")),
                ast.SimpleRow(ast.AttrRef("DATAFLOW0000", "_2", "_2"))
              )
            ),
            ast.StructRow(
              Seq("_1", "_2"),
              Seq(
                ast.SimpleRow(ast.AttrRef("DATAFLOW0001", "_1", "_1")),
                ast.SimpleRow(ast.AttrRef("DATAFLOW0001", "_2", "_2"))
              )
            )
          ))
      case ast.TableScan(tbl, _) => {
        val keys = (1 to u.length).map(x => "_" + x).toList
        val vals = (1 to u.length).map(x => ast.SimpleRow(ast.AttrRef(tbl, "_" + x, "_" + x)))
        ast.StructRow(
          keys,
          vals
        )
      }
      case _ => ast.StructRow(Seq("_1", "_2"), Seq(ast.SimpleRow(ast.AttrRef("DATAFLOW0000", "_1", "_1")),
        ast.SimpleRow(ast.AttrRef("DATAFLOW0001", "_2", "_2"))))
    }

    //val x = m.ttag.tpe
    /*ast.StructRow(
      Seq(
        "_1",
        "_2"),
      Seq(
        ast.Row(ast.StructRow(Seq("_1"),Seq(ast.AttrRef("DATAFLOW0001", "_1", "_1")),
        ast.SimpleRow(ast.AttrRef("DATAFLOW0001", "_1", "_1"))
      )
    )*/
    // TODO: construct via reflection API from the m TypeTag
  }

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  def fold[B: Meta](agg: Alg[A, B]): B =
    ???

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  def map[B: Meta](f: (A) => B): DataBag[B] = {

    println(f.toString())
    /*println(f)

    val func = typecheck(reify {
      () =>
        (xs: (Double, Double, Int, Double, Double)) =>
          scala.math.sqrt(scala.math.pow(xs._2 - xs._4, 2) + scala.math.pow(xs._3 - xs._5, 2))
    }.tree)

    new CoGaDBTable(new UDFTransformer(
      MapUDFClosure(func, Map[String, String]("xs" -> "DATAFLOW0000"),
        rep)).transform
    )*/
    new CoGaDBTable[B](rep)
    ???
  }

  def flatMap[B: Meta](f: (A) => DataBag[B]): DataBag[B] =
    ???

  def withFilter(p: (A) => Boolean): DataBag[A] = {
    println()
    new CoGaDBTable[A](rep)
    ???
  }

  // -----------------------------------------------------
  // Grouping
  // -----------------------------------------------------

  def groupBy[K: Meta](k: (A) => K): DataBag[Group[K, DataBag[A]]] = {
    ???
    //new CoGaDBTable(ast.GroupBy(k,Seq(ast.AggFuncSimple("SUM",ast.AttrRef("DATAFLOW0000","_1","_1"),"_1")),rep))
  }

  // -----------------------------------------------------
  // Set operations
  // -----------------------------------------------------

  def union(that: DataBag[A]): DataBag[A] =
    that match {
      case that: CoGaDBTable[A] => new CoGaDBTable[A](that.rep) // replace with union
      case _ => new CoGaDBTable[A](ast.TableScan(""))
    }

  def distinct: DataBag[A] =
    ???


  // -----------------------------------------------------
  // Partition-based Ops
  // -----------------------------------------------------

  def sample(k: Int, seed: Long = 5394826801L): Vector[A] =
    ???

  def zipWithIndex(): DataBag[(A, Long)] =
    ???

  // -----------------------------------------------------
  // Sinks
  // -----------------------------------------------------

  def writeCSV(path: String, format: CSV)(implicit converter: CSVConverter[A]): Unit =
    CSVScalaSupport(format).write(path)(collect())

  def writeText(path: String): Unit =
    TextSupport.write(path)(collect() map (_.toString))

  def writeParquet(path: String, format: Parquet)(implicit converter: ParquetConverter[A]): Unit =
    ParquetScalaSupport(format).write(path)(collect())

  def collect(): Seq[A] =
    cogadb.exportSeq(rep)


  // -----------------------------------------------------
  // Helpers
  // -----------------------------------------------------
  private[emmalanguage] def store()(
    implicit cogadb: CoGaDB
  ): DataBag[A] = new CoGaDBTable[A](cogadb.saveSeq(rep))

  private[emmalanguage] def ref(field: String): ast.AttrRef =
    ref(field, field)

  private[emmalanguage] def ref(field: String, alias: String): ast.AttrRef =
    resolve(field, alias)(rep)

  private def resolve(field: String, alias: String)(node: ast.Op): ast.AttrRef =
    node match {
      case ast.ImportFromCsv(tablename, _, _, schema) =>
        schema.collectFirst({
          case ast.SchemaAttr(_, `field`) =>
            ast.AttrRef(tablename.toUpperCase, field, alias)
        }).get
      case ast.TableScan(tablename, version) =>
        ast.AttrRef(tablename, field, alias, version)
      case ast.Projection(attrRef, _) =>
        attrRef.collectFirst({
          case ref@ast.AttrRef(table, col, `field`, version) =>
            ref
        }).get
      case ast.Root(child) =>
        resolve(field, alias)(child)
      case ast.Sort(_, child) =>
        resolve(field, alias)(child)
      case ast.GroupBy(_, _, child) =>
        resolve(field, alias)(child)
      case ast.Selection(_, child) =>
        resolve(field, alias)(child)
      case ast.ExportToCsv(_, _, child) =>
        resolve(field, alias)(child)
      case ast.MaterializeResult(_, _, child) =>
        resolve(field, alias)(child)
      case _ =>
        throw new IllegalArgumentException
    }

  private[emmalanguage] def refTable(): String =
    resolveTable()(rep)

  private def resolveTable()(node: ast.Op): String =
    node match {
      case ast.TableScan(tablename, version) =>
        tablename
      case ast.Root(child) =>
        resolveTable()(child)
      case ast.Sort(_, child) =>
        resolveTable()(child)
      case ast.GroupBy(_, _, child) =>
        resolveTable()(child)
      case ast.Selection(_, child) =>
        resolveTable()(child)
      case ast.ExportToCsv(_, _, child) =>
        resolveTable()(child)
      case ast.MaterializeResult(_, _, child) =>
        resolveTable()(child)
      case ast.Projection(_, child) =>
        resolveTable()(child)
      case MapUdf(_, _, child) =>
        resolveTable()(child)
      case _ =>
        print(node)
        throw new IllegalArgumentException
    }


  private[emmalanguage] def fixMapped()(node: ast.Op): ast.Op =
    node

}

object CoGaDBTable extends DataBagCompanion[CoGaDB] {

  // ---------------------------------------------------------------------------
  // Constructors
  // ---------------------------------------------------------------------------

  def empty[A: Meta](
    implicit cogadb: CoGaDB
  ): DataBag[A] = CoGaDBTable.apply(Seq.empty[A])

  def apply[A: Meta](values: Seq[A])(
    implicit cogadb: CoGaDB
  ): DataBag[A] = cogadb.importSeq(values)

  def readText(path: String)(
    implicit cogadb: CoGaDB
  ): DataBag[String] = DataBag(TextSupport.read(path).toStream)

  def readCSV[A: Meta : CSVConverter](path: String, format: CSV)(
    implicit cogadb: CoGaDB
  ): DataBag[A] = DataBag(CSVScalaSupport(format).read(path).toStream)

  def readParquet[A: Meta : ParquetConverter](path: String, format: Parquet)(
    implicit cogadb: CoGaDB
  ): DataBag[A] = DataBag(ParquetScalaSupport(format).read(path).toStream)

  // ---------------------------------------------------------------------------
  // Implicit Rep -> DataBag conversion
  // ---------------------------------------------------------------------------

  private implicit def wrap[A: Meta](rep: ast.Op)(implicit cogadb: CoGaDB): DataBag[A] =
    new CoGaDBTable[A](rep)
}
