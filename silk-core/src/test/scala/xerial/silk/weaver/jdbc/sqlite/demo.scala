import xerial.silk._

object Workflow {

  val a = sql"""
create table A (id integer, name string)
"""

  val b = sql"""
insert into A values((1, 'leo'))
""" dependsOn a

  val selectAll = sql"""
select * from A
where ...
group by ...
...
...
..

""" dependsOn b

}

