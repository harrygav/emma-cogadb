import scala.reflect.runtime.universe._

//val tt = Tuple2((1, 1), (1, "a"))

/*val ss = Tuple2(1, 2)*/
val x = typeTag[(((Int,String),Int),(Int,String))]

def paramInfo[T](tag: TypeTag[T]): Unit = {
  val targs = tag.tpe match {
    case TypeRef(_, _, args) =>
      //println(args.head.<:<(typeOf[Product]))
      //println(args)
      args.map({
        a =>
          if (a <:< typeOf[Product]) {
            println(s"`$a` is product type")
            paramInfo(a)
            1
          }
          else {
            println(s"`$a` not a product type")
            2
          }

      })
      //println(args)
      args
  }
  //println(s"type of $x has type arguments $targs")
}

/*def meth[A : TypeTag](xs: List[A]) = typeOf[A] match {
  case t if t =:= typeOf[Product] => "list of ints"
  //case t if t <:< typeOf[Foo] => "list of foos"
}*/

paramInfo(x)

//val a = List(1,3,2)

