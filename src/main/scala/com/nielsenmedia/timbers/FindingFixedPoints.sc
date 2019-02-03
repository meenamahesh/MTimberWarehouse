def abs(x: Double) = if(x < 0) -x else x

val tolerance = 0.0001

def isCloseEnough(x: Double, y: Double) =
  abs((x-y)/x)/x  < tolerance

def fixedPoint(f: Double => Double)(firstGuess: Double) = {
  def iterate(guess: Double): Double = {
    val next = f(guess)
    if(isCloseEnough(guess, next)) next
    else iterate(next)
  }
  iterate(firstGuess)
}

def sqrt(x: Double) = fixedPoint(y => (y +x/y)/2)(1.0)

sqrt(4)
sqrt(2)
sqrt(100)


// with average damping -- illustrating return of function
def averageDamp(f: Double => Double)(x: Double) = (x + f(x)) / 2

def squareroot(x: Double) = fixedPoint(averageDamp(y => x / y))(1)
squareroot(2)
squareroot(100)

