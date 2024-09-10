object Queries {
  def killJackSparrow(t: Table): Option[Table] = queryT((Some(t), "FILTER", Field("name", (x: String)  => x != "Jack")))

  def insertLinesThenSort(db: Database): Option[Table] = {
    val str = "name Ana, age 93, CNP 455550555\nname Diana, age 33, CNP 255532142\nname Tatiana, age 55, CNP 655532132\nname Rosmaria, age 12, CNP 855532172"
    val maps = str.split("\n")
    val rows: Tabular = maps.map(_.split(", ").foldLeft(Map[String, String]())((acc, x) => {
      val parts = x.split(" ")
      acc + (parts.head -> parts.tail.head)
    })).toList

    val tableName = "Inserted Fellas"
    val column = "age"
    queryT((queryT(queryDB((queryDB((Some(db), "CREATE", tableName)), "SELECT", List(tableName))) match {
      case None => None
      case Some(db) => Some(db.tables.head)
    }, "INSERT", rows), "SORT", column))
  }

  def youngAdultHobbiesJ(db: Database): Option[Table] = {
    val t1 = "People"
    val t2 = "Hobbies"
    val col1 = "name"
    val col2 = "hobby"
    val col3 = "age"
    queryT((queryT((queryDB((Some(db), "JOIN", t1, col1, t2, col1)) match {
      case None => None
      case Some(d) => Some(d.tables.head)
    }, "FILTER", (col3, (age: String) => age != "" && age.toInt < 25) && (col1, (name: String) => name.charAt(0) == 'J') && (col2, (hobby: String) => hobby != ""))),
      "EXTRACT", List(col1, col2)))
  }
}
