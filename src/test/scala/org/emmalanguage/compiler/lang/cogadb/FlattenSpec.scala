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

import org.scalatest.FreeSpec
import org.scalatest.Matchers


class FlattenSpec extends FreeSpec with Matchers {


  "Test" in {

    val a = ast.StructRef(Seq(
      ("a", ast.AttrRef("a", "firstcol", "firstcol", 1))
    )
    )


    val refs = Seq(a)
    val act = ast.flatten(refs)
    val exp = Seq((ast.AttrRef("a", "firstcol", "firstcol", 1)))
    
    act should contain theSameElementsAs exp


  }

}
