def scalarProduct(xs: Vector[Double], ys: Vector[Double]): Double =
  (xs zip ys).map(xy => xy._1 * xy._2).sum

//Patern matching
def scalarProduct2(xs: Vector[Double], ys: Vector[Double]): Double =
  (xs zip ys).map{case(x,y) => x*y}.sum

def scalaProduct3(xs: Vector[Double], ys: Vector[Double]): Double =
  (for((x,y) <- xs zip ys) yield x * y).sum


def isPrime(n: Int): Boolean = (2 until n) forall(d => n%d !=0)
isPrime(31)


///Combinatorial Search and For-Expressions

val n=7
((1 until n).map(i => (1 until i).map(j => (i, j)))).flatten
// OR
(1 until n) flatMap (i => (1 until i).map(j => (i, j)))


(1 until n) flatMap (i => (1 until i).map(j => (i, j))) filter(pair => isPrime(pair._1 + pair._2))
//OR
for(i <- 1 until n; j <- 1 until i; if isPrime(i+j)) yield(i,j)

//For-Expressions

case class Person(name: String, age: Int)
val persons:List[Person] = List()
for(p <- persons if p.age > 20) yield p.name
//OR
persons filter(p => p.age>20) map(p=> p.name)

