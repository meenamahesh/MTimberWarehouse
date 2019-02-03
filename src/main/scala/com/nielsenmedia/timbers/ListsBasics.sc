val lst = List(1, 2, 3, 4)

lst.foldLeft(0)((acc, nxt) => acc+nxt)
lst.foldLeft(1)((acc, nxt) => acc*nxt)
lst.foldLeft(1)((acc, nxt) =>
{ println(s"$nxt")
  acc*nxt} )


lst.foldRight(0)((acc, nxt) => acc+nxt)
lst.foldRight(1)((acc, nxt) => acc*nxt)

lst.foldRight(1)((acc, nxt) =>
{ println(s"$nxt")
  acc*nxt} )

val alphalst = List("ab", "cd", "ef")
alphalst.foldLeft("")((a,b) => a+b)



def contains[A](lst: List[A], item:A) = lst.foldLeft(false)((a, b) => a || b == item)


def reverse[A](lst: List[A]): List[A] = lst.foldLeft(List[A]())((a,b) => b :: a)

reverse(lst)
