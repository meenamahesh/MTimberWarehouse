
def product(f: Int => Int)(a: Int, b: Int): Int =
  if (a>b) 1
  else f(a) * product(f)(a+1, b)

product(x => x*x)(3,4)

def mapReduce(f: Int => Int, combine: (Int, Int) => Int, initial: Int)(a: Int, b: Int): Int =
  if(a > b) initial
else combine(f(a), mapReduce(f, combine, initial)(a+1, b))


// redefine product method
def product1(f: Int=> Int)(a: Int, b: Int) = mapReduce(f, (x, y) => x*y , 1)(a, b)

def fact(n: Int) = product1(x => x)(1, n)
fact(5)

