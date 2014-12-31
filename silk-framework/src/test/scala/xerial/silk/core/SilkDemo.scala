package xerial.silk.core




trait SilkDemo {

  import xerial.silk.Silk._

  case class Job(id:Long, uid:Int, query:String, time:Long)
  case class Email(uid:Int, email:String)

  // Load tables
  val jobs = loadAs[Job]("job.mpc")
  val userEmails = loadAs[Email]("email_list.mpc")

  // Procedural Style: Describes how to process the data

  // Nested loops
  for(j <- jobs;
      e <- userEmails
      if j.uid == e.uid) yield
      (j, e.email)

  // In functional style
  jobs.flatMap { j =>
    userEmails.collect {
      case e if j.uid == e.uid =>
        (j, e.email)
    }
  }

  // An efficient version
  val m = userEmails.map{e => e.uid -> e.email}
    .toMap[Int, String]

  jobs
    .filter{j => m.contains(j.uid)}
    .map{j => (j, m(j.uid))}


  // Declarative Style: Describe what to do with the data

  // Join
  jobs.join(userEmails, {j:Job => j.uid}, {e:Email => e.uid})

  // SQL
  sql"select * from job j, user_email e where j.uid = e.uid"



}
