import scala.language.implicitConversions

trait PP_SQL_DB{
  def eval: Option[Database]
}

case class CreateTable(database: Database, tableName: String) extends PP_SQL_DB{
  def eval: Option[Database] = Some(database.create(tableName))
}

case class DropTable(database: Database, tableName: String) extends PP_SQL_DB{
  def eval: Option[Database] = Some(database.drop(tableName))
}

implicit def PP_SQL_DB_Create_Drop(t: (Option[Database], String, String)): Option[PP_SQL_DB] = {
  t match
    case (None, _, _) => None
    case (Some(d), op, name) =>
      if (op == "DROP") Some(DropTable(d, name))
      else if (op == "CREATE") Some(CreateTable(d, name))
      else None
}

case class SelectTables(database: Database, tableNames: List[String]) extends PP_SQL_DB{
  def eval: Option[Database] = database.selectTables(tableNames)
}

implicit def PP_SQL_DB_Select(t: (Option[Database], String, List[String])): Option[PP_SQL_DB] = {
  t match
    case (None, _, _) => None
    case (Some(d), op, list) =>
      if (op == "SELECT") Some(SelectTables(d, list))
      else None
}

case class JoinTables(database: Database, table1: String, column1: String, table2: String, column2: String) extends PP_SQL_DB{
  def eval: Option[Database] = {
    val table = database.join(table1, column1, table2, column2)
    table match
      case None => None
      case Some(t) => Some(Database(List(t)))
  }
}

implicit def PP_SQL_DB_Join(t: (Option[Database], String, String, String, String, String)): Option[PP_SQL_DB] = {
  t match
    case (None, _, _, _, _, _) => None
    case (Some(d), op, t1, c1, t2, c2) =>
      if (op == "JOIN") Some(JoinTables(d, t1, c1, t2, c2))
      else None
}

trait PP_SQL_Table{
  def eval: Option[Table]
}

case class InsertRow(table:Table, values: Tabular) extends PP_SQL_Table{
  def eval: Option[Table] = Some(values.foldRight(table)((row, acc) => acc.insert(row)))
}

implicit def PP_SQL_Table_Insert(t: (Option[Table], String, Tabular)): Option[PP_SQL_Table] = {
  t match
    case (None, _, _) => None
    case (Some(t), op, values) =>
      if (op == "INSERT") Some(InsertRow(t, values))
      else None
}

case class UpdateRow(table: Table, condition: FilterCond, updates: Map[String, String]) extends PP_SQL_Table{
  def eval: Option[Table] = Some(table.update(condition, updates))
}

implicit def PP_SQL_Table_Update(t: (Option[Table], String, FilterCond, Map[String, String])): Option[PP_SQL_Table] = {
  t match
    case (None, _, _, _) => None
    case (Some(t), op, condition, updates) =>
      if (op == "UPDATE") Some(UpdateRow(t, condition, updates))
      else None
}

case class SortTable(table: Table, column: String) extends PP_SQL_Table{
  def eval: Option[Table] = Some(table.sort(column))
}

implicit def PP_SQL_Table_Sort(t: (Option[Table], String, String)): Option[PP_SQL_Table] = {
  t match
    case (None, _, _) => None
    case (Some(t), op, column) =>
      if (op == "SORT") Some(SortTable(t, column))
      else None
}

case class DeleteRow(table: Table, row: Row) extends PP_SQL_Table{
  def eval: Option[Table] = Some(table.delete(row))
}

implicit def PP_SQL_Table_Delete(t: (Option[Table], String, Row)): Option[PP_SQL_Table] = {
  t match
    case (None, _, _) => None
    case (Some(t), op, row) =>
      if (op == "DELETE") Some(DeleteRow(t, row))
      else None
}

case class FilterRows(table: Table, condition: FilterCond) extends PP_SQL_Table{
  def eval: Option[Table] = Some(table.filter(condition))
}

implicit def PP_SQL_Table_Filter(t: (Option[Table], String, FilterCond)): Option[PP_SQL_Table] = {
  t match
    case (None, _, _) => None
    case (Some(t), op, condition) =>
      if (op == "FILTER") Some(FilterRows(t, condition))
      else None
}

case class SelectColumns(table: Table, columns: List[String]) extends PP_SQL_Table{
  def eval: Option[Table] = {
    Some(table.select(columns))
  }
}

implicit def PP_SQL_Table_Select(t: (Option[Table], String, List[String])): Option[PP_SQL_Table] = {
  t match
    case (None, _, _) => None
    case (Some(t), op, columns) =>
      if (op == "EXTRACT") Some(SelectColumns(t, columns))
      else None
}

def queryT(p: Option[PP_SQL_Table]): Option[Table] = {
  p match
    case None => None
    case Some(query) => query.eval
}
def queryDB(p: Option[PP_SQL_DB]): Option[Database] = {
  p match
    case None => None
    case Some(query) => query.eval
}