package xerial.silk.core


object FContext {

  val empty = new FContext(this.getClass, "none", None, None, "unknown", 1, 0);

}


case class FContext(owner: Class[_],
                    name: String, localValName: Option[String], parentValName: Option[String], source: String, line: Int,
                    column: Int) {

  def baseTrait: Class[_] = {

    // If the class name contains $anonfun, it is a compiler generated class.
    // If it contains $anon, it is a mixed-in trait
    val isAnonFun = owner.getSimpleName.contains("$anon")
    if (!isAnonFun) {
      owner
    }
    else {
      // If the owner is a mix-in class
      owner.getInterfaces.headOption orElse
        Option(owner.getSuperclass) getOrElse
        owner
    }
  }

  private def format(op: Option[String]) = {
    if (op.isDefined) {
      s"${op.get}:"
    }
    else {
      ""
    }
  }

  override def toString = {
    val className = baseTrait.getSimpleName.replaceAll("\\$", "")
    val method = if (name == "<constructor>") "" else s".${name}"
    val targetName = localValName.filter(_ != name).map(lv => s"${method}:${lv}").getOrElse(method)
    val parentStr = if(parentValName.isDefined) s" (parent:${parentValName}) " else ""
    s"${className}${targetName}${parentStr} [L$line:$column]"
  }

  def refID: String = {
    s"${owner.getName}:${format(localValName)}${format(parentValName)}$name"
  }
}

