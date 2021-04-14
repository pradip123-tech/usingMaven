
{
  def squareIt(x: Int) = {
    x * x
  }
  {
    def transformInt(x: Int, f: Int => Int): Int = {
      f(x)
    }

    transformInt(2, squareIt)
  }

  val a = Array(1,2,3,4,5)
  //println(a.mkString(","))

  //for (i <- a) println(i)

  val b = List(1,2,3,4,5)

  println(b.tail)


  val c =("pm",20,true)
  println(c._2)

  val rng = 1 to 10000
  for(i <- rng) println(i)
}