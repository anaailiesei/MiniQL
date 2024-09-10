import scala.language.implicitConversions

trait FilterCond {def eval(r: Row): Option[Boolean]}

case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    val value = r get colName
    value match
      case None => None
      case Some(s) => Some(predicate(s))
  }
}

case class Compound(op: (Boolean, Boolean) => Boolean, conditions: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    val results = conditions.map(_.eval(r))
    results.reduce((x, y) => {
      (x, y) match
        case (None, _) | (_, None) => None
        case (Some(a), Some(b)) => Some(op(a, b))
    })
  }
}

case class Not(f: FilterCond) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    f.eval(r) match
      case None => None
      case Some(cond) => Some(!cond)
  }
}

def And(f1: FilterCond, f2: FilterCond): FilterCond = Compound((b1, b2) => b1 && b2, List(f1, f2))
def Or(f1: FilterCond, f2: FilterCond): FilterCond = Compound((b1, b2) => b1 || b2, List(f1, f2))
def Equal(f1: FilterCond, f2: FilterCond): FilterCond = Compound((b1, b2) => b1 == b2, List(f1, f2))

case class Any(fs: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = Compound((b1, b2) => b1 || b2, fs).eval(r)
}

case class All(fs: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = Compound((b1, b2) => b1 && b2, fs).eval(r)
}

implicit def tuple2Field(t: (String, String => Boolean)): Field = Field(t._1, t._2)

extension (f: FilterCond) {
  def ===(other: FilterCond) = Equal(f, other)
  def &&(other: FilterCond) = And(f, other)
  def ||(other: FilterCond) = Or(f, other)
  def !! = Not(f)
}