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
import api._

object Playground extends CoGaDBAware {

  def main(args: Array[String]): Unit =

  //withCoGaDB(implicit cogadb => emma.onCoGaDB("debug.conf") {
    withCoGaDB(implicit cogadb => emma.onCoGaDB("reference.emma.onCoGaDB.conf") {

      val xs = DataBag(Seq(1 -> 2, 2 -> 2, 3 -> 4))
      val ys = DataBag(Seq((1, "foo", "hallo"), (2, "bar", "bier")))
      val as = DataBag(Seq(1 -> "a", 1 -> "b", 3 -> "c"))
      /*val zs = for {
        x <- xs
        y <- ys
        //a <- as
        if x._1 == y._1
        //if a._1 == x._1
      } yield {
        val newy = y._1*2
        (x._1,newy)
      }*/

      /*val zs =  {
        for {
          Group(k, g) <- as.groupBy(_._1)
        } yield {
          val x = g.count(_._2=="foo")
          k -> x
        }
      }*/

      val zs = for {
        x <- xs
        if x._1 > 1
      } yield (x._1*18)

      //val zs = xs.map(x => (x._1 * 3, x._2))
      //val t = zs.collect().toList
      /*val zs = for {
        x <- xs
        if (x._1) > 1
      } yield {
        def timesThree(x: Int): Int = x*3
        val mult = timesThree(x._1)
        (x._1, mult)
      }*/


      val t = zs.collect().toList

      //val s = as.collect().toList
      //print(t+""+s)
      print(t)
      //val csv = CSV(delimiter = ',')
      //zs.writeText("/home/harry/Desktop/test.csv")

    })
}
