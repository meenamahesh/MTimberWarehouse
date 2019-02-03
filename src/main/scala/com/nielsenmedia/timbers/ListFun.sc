val nums = List(2, -4, 5, 7, 1)

nums filter(x => x >0)
nums filterNot(x => x > 0)
nums partition(x => x > 0)

nums takeWhile(x=> x>0)
nums dropWhile(x=> x>0)
nums span(x=> x>0)

val data=List("a", "a", "a", "b", "c", "c", "a")
//output List(List("a", "a", "a"), List("b"), List("c", "c"), List("a"))
def pack[T](xs: List[T]): List[List[T]] = xs match {
  case Nil =>Nil
  case x :: xs1 =>
    val (first, rest) = xs span (y => y ==x )
    first :: pack(rest)
}
pack(data)

// encode functio should return List((a, 3), (b, 1), (c, 2), (a, 1))
def encode[T](xs: List[T]): List[(T, Int)] =
  pack(xs) map (ys => (ys.head, ys.length))

encode(data)

