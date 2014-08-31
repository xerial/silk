package xerial.silk.example

import xerial.silk.{SilkSeq, Silk}
import Silk._


class Basic {


  case class PeopleExt(id:Int, name:String, title:String, dept:Dept)


  case class PeopleDept(pid:Int, did:Int, since:String)



  val dept = loadAs[Dept]("dept.dat")
  val people_dept = loadAs[PeopleDept]("people_dept.dat")

//  people
//    .join(people_dept, {p:People => p.id}, {pd:PeopleDept => pd.pid})

  object People {
    def apply(l:String) : People = null
  }

  case class People(id:Int, name:String, title:String, dept:Dept)
  case class Dept(id:Int, name:String)

  val people = loadAs[People]("people.dat")
  val engineers = people.filter(_.title == "Software Engineer")

  val engineerNames =
    engineers
      .filter(_.dept.name == "JP")
      .map(_.name)

  val numEngineersPerDept = engineers
    .groupBy(_.dept)
    .map{case (d, lst) => (d.name, lst.size) }


  val engineerNames2 =
    for(p <- engineers if p.dept.name == "JP") yield p.name



  for((dept, lst) <- people.groupBy(_.dept.name)) yield
    lst.filter(_.title == "Software Engineer")
      .map(p => s"$dept ${p.name}")



  val A = Seq(1, 2).toSilk
  val f = { (i:Int) => i }
  val g = { (i:Int) => i}

  val B = A.map(f)
  val C = B.map(g)

  val pgroup = people.groupBy(_.dept.id)

  import Silk._


  trait Employee {
    // Parsing text data
    def people : SilkSeq[People] =
      openFile("people.txt")
        .lines.map(line => People(line))
  }

  // Reusing a workflow by mixin
  class EmployeeList extends Employee {
    def list = people.map(p => (p.id, p.name))
  }

  // Override the result
  class SampledEmployeeList extends EmployeeList {
    override def people =
      super.people.takeSample(0.01)

    //list function now returns a list of 1% of people
  }




}
