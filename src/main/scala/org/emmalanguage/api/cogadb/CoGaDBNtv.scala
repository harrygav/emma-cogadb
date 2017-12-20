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

import api._
import compiler.lang.cogadb.ast
import runtime.CoGaDB


object CoGaDBNtv {
  import Meta.Projections._

  //----------------------------------------------------------------------------
  // Specialized combinators
  //----------------------------------------------------------------------------
  def map[A: Meta, B: Meta](f: ast.Row => ast.Row)(xs: DataBag[A])(
    implicit cogadb: CoGaDB
  ): DataBag[B] = xs match{
    case table(ox,x) => {

     ???
    }
    case _ => throw new IllegalArgumentException("Not a table")
  }

  def select[A: Meta](p: ast.Row => ast.Row)(xs: DataBag[A])(
    implicit cogadb: CoGaDB
  ): DataBag[A] = xs match {
    case table(ox, x) => p(x).attr match {
      case asPreds(ps) =>
        // all fields in the result Row are simple predicates
        table(ast.Selection(ps, ox))
      case _ =>
        // not all fields are simple predicates - TODO: compile as UDF
        throw new IllegalArgumentException("Select: Not all fields in result of `p` are predicates")
    }
    case _ => throw new IllegalArgumentException("Selection failure")
  }

  def project[A: Meta, B: Meta](f: ast.Row => ast.Row)(xs: DataBag[A])(
    implicit cogadb: CoGaDB
  ): DataBag[B] = xs match {
    case table(ox, x) => f(x).attr match {
      case asAttrs(as) =>
        // all fields in the result Row are simple attributes
        table(ast.Projection(as, ox))
      case _ =>
        // not all fields are simple predicates - TODO: compile as UDF
        throw new IllegalArgumentException("Not all fields in result of `f` are predicates")
    }
    case _ => throw new IllegalArgumentException("Projection failure")
  }

  def equiJoin[A: Meta, B: Meta, K: Meta](
    kx: ast.Row => ast.Row, ky: ast.Row => ast.Row)(xs: DataBag[A], ys: DataBag[B]
  )(implicit cogadb: CoGaDB): DataBag[(A, B)] = (xs, ys) match {
    case (table(ox, x), table(oy, y)) => (kx(x).attr, ky(y).attr) match {
      case (asAttrs(ax), asAttrs(ay)) =>
        table(ast.Join("INNER_JOIN", zeq(ax, ay), ox, oy))
      case _ =>
        throw new IllegalArgumentException("Equi join failure")
    }
  }

  def cross[A: Meta, B: Meta](
    xs: DataBag[A], ys: DataBag[B]
  )(implicit s: CoGaDB): DataBag[(A, B)] = (xs, ys) match {
    case (table(ox, _), table(oy, _)) =>
      table(ast.CrossJoin(ox, oy))
  }


  //----------------------------------------------------------------------------
  // Helper Objects and Methods
  //----------------------------------------------------------------------------

  private def and(conjs: Seq[ast.AttrRef]): ast.Predicate = ???

  private def zeq(lhs: Seq[ast.AttrRef], rhs: Seq[ast.AttrRef]): Seq[ast.Predicate] =
    for ((l, r) <- lhs zip rhs) yield ast.ColCol(l, r, ast.Equal)

  private object table {
    def apply[A: Meta](
      rep: ast.Op
    )(implicit cogadb: CoGaDB): DataBag[A] = new CoGaDBTable(rep)

    def unapply[A: Meta](
      bag: DataBag[A]
    )(implicit cogadb: CoGaDB): Option[(ast.Op, ast.Row)] = bag match {
      case bag: CoGaDBTable[A] => Some((bag.rep, bag.col))
      case _ => None
    }
  }

  private object asAttrs {
    def unapply(xs: Seq[ast.Expr]): Option[Seq[ast.AttrRef]] =
      if (!xs.forall(_.isInstanceOf[ast.AttrRef])) None
      else Some(xs.map(_.asInstanceOf[ast.AttrRef]))
  }

  private object asPreds {
    def unapply(xs: Seq[ast.Expr]): Option[Seq[ast.Predicate]] =
      if (!xs.forall(_.isInstanceOf[ast.Predicate])) None
      else Some(xs.map(_.asInstanceOf[ast.Predicate]))
  }

}