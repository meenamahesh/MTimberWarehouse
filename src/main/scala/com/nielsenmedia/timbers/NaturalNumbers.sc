/// Peono numbers

abstract class Nat {
  def isZero: Boolean
  def predecessor: Nat
  def successr: Nat = new Succ(this)
  def +(that: Nat): Nat
  def -(that: Nat): Nat
}


object Zero extends Nat {
  override def isZero: Boolean = true

  override def predecessor: Nat = throw new Error("0.predecessor")

  def +(that: Nat): Nat = that
  def -(that: Nat): Nat = if(that.isZero) this else throw new Error("negative number")

}

class Succ(n : Nat) extends Nat {
  def isZero = false

  override def predecessor: Nat = n
  def +(that: Nat): Nat = new Succ(n + that)
  def -(that: Nat): Nat = if(that.isZero) this else n - that.predecessor //new Succ(n - that)

}