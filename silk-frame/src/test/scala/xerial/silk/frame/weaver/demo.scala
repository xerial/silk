package xerial.silk.frame.weaver

import xerial.frame.weaver.SQLite
import xerial.silk._

import scala.util.Random

object Workflow {

  def db = SQLite("target/mydb.sqlite")

  def cleanup = db.dropTableIfExists("A")
  def a = db.createTable("A", "id integer, name string") dependsOn cleanup

  def insertTasks = for(i <- 0 until 5) yield {
    val name = ('A' + Random.nextInt(25)).toChar
    db.sql(s"insert into A values(${i}, '${name}')") dependsOn a
  }

  def selectAll = db.table("A").selectAll dependsOn insertTasks

}




