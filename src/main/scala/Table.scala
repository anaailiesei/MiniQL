import scala.annotation.tailrec
import scala.collection.immutable.ListMap

type Row = Map[String, String]
type Tabular = List[Row]

case class Table (tableName: String, tableData: Tabular) {

  override def toString: String = {
    val values = tableData.map(_.map((_, v) => v))
    val str = (header :: values).map(_.foldRight("")((x, acc) =>
      if (acc == "") x + "\n"
      else x + ',' + acc)).foldRight("")((x, acc) => x + acc)
    str.strip()
  }

  def insert(row: Row): Table = {
    if (data.contains(row)) this
    else Table.apply(name, data :+ row)
  }

  def delete(row: Row): Table = Table.apply(name, data.filter(curr_row => curr_row != row))

  def sort(column: String): Table = Table.apply(name, data.sortBy(row => row.get(column)))

  def update(f: FilterCond, updates: Map[String, String]): Table = {
    val updatedData = data.map(row => {
      f.eval(row) match
        case None | Some(false) => row
        case Some(true) => row ++ updates
    })
    Table(name, updatedData)
  }

  def filter(f: FilterCond): Table = {
    val filteredData = data.filter(row => {
      f.eval(row) match
        case None => false
        case Some(b) => b
    })
    Table(name, filteredData)
  }

  def select(columns: List[String]): Table = {
    val emptyRow: Row = ListMap()
    val cols = data.map(row => columns.foldLeft(emptyRow)((acc, col) => acc + (col -> row.getOrElse(col, ""))))
    Table.apply(name, cols)
  }

  def header: List[String] = tableData match
    case List() => List()
    case _ => tableData.head.map((k, _) => k).toList
  def data: Tabular = tableData
  def name: String = tableName
}

object Table {
  def apply(name: String, s: String): Table = {
    val lines = s.split('\n')
    val table = lines.map(_.split(','))
    val header = table.head
    val data = table.tail

    val emptyMap: Row = Map()
    val tableData = data.map(header.zip(_).foldRight(emptyMap)((pair, acc) => acc + (pair._1 -> pair._2))).toList
    new Table(name, tableData)
  }

  def apply(name: String, tableData: Tabular) = new Table(name, tableData)
}

extension (table: Table) {
  def apply(i: Int): Table = new Table(table.name, List(table.data(i))) // Implement indexing here, find the right function to override
}
