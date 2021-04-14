if (1>3){
  println("Hello")
}
else{
  println("there")
}

val num=5
num match {
  case 1 => println("one")
  case _ => println("else")
}
for (x <- 1 to 10){
  val squared = x*x
  println(s"The result is $squared")
}
