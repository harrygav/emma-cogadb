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

import compiler.lang.cogadb.ast
import compiler.udf.UDFTransformer
import compiler.udf.common.MapUDFClosure
import runtime.CoGaDB

import org.scalatest._

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

/**
 * A spec for the TPC-H Queries for the CoGaDB backend
 * In order to be able to run the tests, the
 * 1) TPC-H reference databases must be installed,
 * as described in the README
 * The script is located in the CoGaDB directory:
 * `utility_scripts/setup_reference_databases.sh`
 * 2) The CoGaDB startup script e.g. `test/resources/cogadb/tpch.coga`
 * must specify the correct path to the database files e.g.
 * `set path_to_database=/home/user/cogadb_reference_dbs/cogadb_reference_databases_v1/tpch_sf1/`
 * but also load the database from the path using following commands:
 * `set table_loader_mode=disk`
 * `loaddatabase`
 */
class SaveSpec extends FreeSpec with Matchers with CoGaDBSpec {


  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  private def typecheck(ast: Tree): Tree = tb.typecheck(ast)


  "Renaming TableScan" in withCoGaDB { implicit cogadb: CoGaDB =>

    val as = new CoGaDBTable[(Int, String)](cogadb.importSeq(Seq((1, "Foo"), (2, "Hello"))))
    val bs = new CoGaDBTable[(Int, String)](cogadb.importSeq(Seq((1, "Bar"), (2, "World"))))

    val crossed = new CoGaDBTable[(Int, String, Int, String)](cogadb.saveSeq( ast.CrossJoin(as.rep,bs.rep)))



    val timesThree = typecheck(reify {
      () => (t: (Int, String, Int, String)) => t._1 * 3
    }.tree)

    val act = new CoGaDBTable[(Int,String,Int)](new UDFTransformer(
      MapUDFClosure(timesThree, Map[String, String]("t" -> crossed.refTable()), crossed.rep)).transform
    ).collect()



    /*val renamed = new CoGaDBTable[String](
      ast.Projection(Seq(ast.AttrRef(as.refTable(), "_2", "_2"), ast.AttrRef(as.refTable(), "_1", "_1")), as.rep)
    ).store()*/
    /*
    val bs = new CoGaDBTable[(Int, String)](cogadb.importSeq(Seq((1, "Foo"), (2, "Hello")))).collect()


    val renamed = new CoGaDBTable[(Int, String)](
      ast.MaterializeResult(
        as.refTable(),false,
      ast.Rename(Seq(ast.AttrRef(as.refTable(),"_2","_1"),ast.AttrRef(as.refTable(),"_1","_2")),as.rep)
      )
    ).collect


    renamed.foreach(println)
    */
    val test = ""
  }


}
