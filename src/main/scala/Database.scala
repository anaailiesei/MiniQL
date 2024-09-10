import scala.collection.immutable.ListMap
import scala.util.control.Breaks.break

case class Database(tables: List[Table]) {
  override def toString: String = tables.toString()

  def create(tableName: String): Database = {
    if (tables.map(t => t.name).contains(tableName)) this
    else Database(tables :+ Table(tableName, List()))
  }

  def drop(tableName: String): Database = {
    if (!tables.map(t => t.name).contains(tableName)) this
    else Database(tables.filter(table => table.name != tableName))
  }

  def selectTables(tableNames: List[String]): Option[Database] = {
    val selectedTables = tableNames.map(name => {
      val filteredTables = tables.filter(table => table.name == name)
      if (filteredTables.isEmpty) break() // instead of return
      else filteredTables.head
    })

    if (selectedTables.length != tableNames.length) None
    else Some(Database(selectedTables))
  }

  def join(table1: String, c1: String, table2: String, c2: String): Option[Table] = {
    def fillEmptyFields(r: Row, header: List[String]): Row = {
      val tuples = header.foldRight(Map[String, String]())((col, acc) => {
        if (col == c1 || col == c2) {
          val entries = r.filter((k, _) => k == c2 || k == c1)
          if (entries.isEmpty) acc + (c1 -> "")
          else acc + (c1 -> entries.head._2)
        } else {
          val entries = r.filter((k, _) => k == col)
          if (entries.isEmpty) acc + (col -> "")
          else acc + (entries.head._1 -> entries.head._2)
        }
      })
      tuples
    }
    def appendRows(r1: Row, r2: Row, header: List[String]): Row = {
      header.map(col => {
        val val1 = r1 get col
        val val2 = r2 get col
        (val1, val2) match
          case (None, None) => (col, "")
          case (Some(a), None) => (col, a)
          case (None, Some(a)) => (col, a)
          case (Some(a), Some(b)) => if (a == b) (col, a) else (col, a + ";" + b)
      }).toMap
    }
    def joinT(t1: Table, t2: Table): Table = {
      val header = t1.header ++ t2.header.filter(c => c != c2 && !t1.header.contains(c))
      val commonRows = t1.data.foldRight(List[Row]())((row1, acc) => {
        val value1 = row1 get c1
        value1 match
          case None => acc
          case Some(field1) => {
            val filtered = t2.filter(Field(c2, _ == field1)).data
            if (filtered.isEmpty) acc
            else appendRows(row1, filtered.head, header) :: acc
          }
      })

      val t1Rows = t1.data.filter(row1 => {
        val value1 = row1 get c1
        value1 match
          case None => false
          case Some(field1) => {
            val filtered = t2.filter(Field(c2, _ == field1)).data
            filtered.isEmpty
          }
      }).map(fillEmptyFields(_, header))

      val t2Rows = t2.data.filter(row2 => {
        val value2 = row2 get c2
        value2 match
          case None => false
          case Some(field2) => {
            val filtered = t1.filter(Field(c1, _ == field2)).data
            filtered.isEmpty
          }
      }).map(fillEmptyFields(_, header))

      Table(t1.name, commonRows ++ t1Rows ++ t2Rows)
    }

    val db = selectTables(List(table1, table2))
    db match
      case None => None
      case Some(a) =>
          if (a(0).data.isEmpty) Some(a(1))
          else if (a(1).data.isEmpty) Some(a(0))
          else Some(joinT(a(0), a(1)))
  }

  // Implement indexing here
  def apply(i: Int): Table = tables(i)
}
