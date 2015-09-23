package demo

import xerial.silk._
import xerial.silk.weaver.jdbc.sqlite.SQLite

object Workflow {

  implicit val db = SQLite("target/mydb.sqlite")

  def cleanup = db.dropTableIfExists("A")

  def a = db.createTable("A", "id integer, name string") dependsOn cleanup

  def b = sql"""
   insert into A values(1, 'leo')
    """ dependsOn a

  def selectAll = db.table("A").selectAll dependsOn b

}

