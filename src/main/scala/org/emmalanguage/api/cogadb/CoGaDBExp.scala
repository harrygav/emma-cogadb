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
package api.cogadb

import compiler.lang.cogadb.ast
import runtime.CoGaDB

//import scala.language.implicitConversions

object CoGaDBExp {

  def struct(names: String*)(vals: ast.Row*): ast.Row =
    ast.StructRow(names, vals)

  def proj(x: ast.Row, name: String)(implicit cogadb: CoGaDB): ast.Row =
    x.proj(name)


  object rowOf {
    def unapply(x: Any): Option[ast.Row] = x match {
      case x: ast.Row => Some(x)
      case x: Int => Some(ast.SimpleRow(ast.IntConst(x)))
      case x: Long => Some(ast.SimpleRow(ast.FloatConst(x)))
      case x: Double => Some(ast.SimpleRow(ast.DoubleConst(x)))
      case x: Boolean => Some(ast.SimpleRow(ast.BoolConst(x.toString)))


      case x: String => Some(ast.SimpleRow(ast.VarCharConst(x)))

      case _ => ??? // TODO
    }
  }

  def eq(x: Any, y: Any)(implicit cogadb: CoGaDB): ast.Row = (x, y) match {
    case (rowOf(ast.SimpleRow(u: ast.AttrRef)), rowOf(ast.SimpleRow(v: ast.Const))) =>
      ast.SimpleRow(ast.ColConst(u, v, ast.Equal))
    case (rowOf(ast.SimpleRow(u: ast.Const)), rowOf(ast.SimpleRow(v: ast.AttrRef))) =>
      ast.SimpleRow(ast.ColConst(v, u, ast.Equal))
    case (rowOf(ast.SimpleRow(u: ast.AttrRef)), rowOf(ast.SimpleRow(v: ast.AttrRef))) =>
      ast.SimpleRow(ast.ColCol(v, u, ast.Equal))
    case (rowOf(us: ast.StructRow), rowOf(vs: ast.StructRow)) =>
      assert(us.attr.size == vs.attr.size, "Comparing structs with different field count")
      ???
    case _ => throw new IllegalArgumentException("Combination not found")
  }

  def gt(x: Any, y: Any)(implicit cogadb: CoGaDB): ast.Row = (x, y) match {
    case (rowOf(ast.SimpleRow(u: ast.AttrRef)), rowOf(ast.SimpleRow(v: ast.Const))) =>
      ast.SimpleRow(ast.ColConst(u, v, ast.GreaterThan))
    case (rowOf(ast.SimpleRow(u: ast.Const)), rowOf(ast.SimpleRow(v: ast.AttrRef))) =>
      ast.SimpleRow(ast.ColConst(v, u, ast.GreaterThan))
    case (rowOf(ast.SimpleRow(u: ast.AttrRef)), rowOf(ast.SimpleRow(v: ast.AttrRef))) =>
      ast.SimpleRow(ast.ColCol(v, u, ast.GreaterThan))
    case _ => throw new IllegalArgumentException("Combination not found")
  }

  def ne(x: Any, y: Any)(implicit cogadb: CoGaDB): ast.Node =
    ???

  def lt(x: Any, y: Any)(implicit cogadb: CoGaDB): ast.Row = (x, y) match {
    case (rowOf(ast.SimpleRow(u: ast.AttrRef)), rowOf(ast.SimpleRow(v: ast.Const))) =>
      ast.SimpleRow(ast.ColConst(u, v, ast.LessThan))
    case (rowOf(ast.SimpleRow(u: ast.Const)), rowOf(ast.SimpleRow(v: ast.AttrRef))) =>
      ast.SimpleRow(ast.ColConst(v, u, ast.LessThan))
    case (rowOf(ast.SimpleRow(u: ast.AttrRef)), rowOf(ast.SimpleRow(v: ast.AttrRef))) =>
      ast.SimpleRow(ast.ColCol(v, u, ast.LessThan))
    case _ => throw new IllegalArgumentException("Combination not found")
  }

  def geq(x: Any, y: Any)(implicit cogadb: CoGaDB): ast.Predicate =
    ???

  def leq(x: Any, y: Any)(implicit cogadb: CoGaDB): ast.Predicate =
    ???

  def not(x: Any)(implicit cogadb: CoGaDB): ast.Node =
    ???

  def or(x: Any, y: Any)(implicit cogadb: CoGaDB): ast.Row = (x, y) match {
    case (rowOf(ast.SimpleRow(u: ast.Predicate)), rowOf(ast.SimpleRow(v: ast.Predicate))) =>
      ast.SimpleRow(ast.Or(Seq(u, v)))
    case _ => throw new IllegalArgumentException(s"Or could not be applide to $x and $y")
  }

  def and(x: Any, y: Any)(implicit cogadb: CoGaDB): ast.Row = (x, y) match {
    case (rowOf(ast.SimpleRow(u: ast.Predicate)), rowOf(ast.SimpleRow(v: ast.Predicate))) =>
      ast.SimpleRow(ast.And(Seq(u, v)))
    case _ => throw new IllegalArgumentException(s"And could not be applide to '$x' and '$y'")
  }

  def plus(x: Any, y: Any)(implicit cogadb: CoGaDB): ast.Node =
    ???

  def minus(x: Any, y: Any)(implicit cogadb: CoGaDB): ast.Node =
    ???

  def multiply(x: Any, y: Any)(implicit cogadb: CoGaDB): ast.Node =
    ???

  def divide(x: Any, y: Any)(implicit cogadb: CoGaDB): ast.Node =
    ???

  def mod(x: Any, y: Any)(implicit cogadb: CoGaDB): ast.Node =
    ???

  def startsWith(x: Any, y: Any)(implicit cogadb: CoGaDB): ast.Node =
    ???


}
