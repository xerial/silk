//--------------------------------------
//
// FContext.scala
// Since: 2013/06/22 13:53
//
//--------------------------------------

package xerial.silk.framework.ops
import scala.language.existentials

/**
 * Function context tells in which function and variable definition this silk operation is used.
 */
case class FContext(owner: Class[_], name: String, localValName: Option[String], parentValName:Option[String], source:String, line:Int, column:Int) {

  def baseTrait : Class[_] = {

    // If the class name contains $anonfun, it is a compiler generated class.
    // If it contains $anon, it is a mixed-in trait
    val isAnonFun = owner.getSimpleName.contains("$anon")
    if(!isAnonFun)
      owner
    else {
      // If the owner is a mix-in class
      owner.getInterfaces.headOption orElse
        Option(owner.getSuperclass) getOrElse
        owner
    }
  }


  private def format(op:Option[String]) = {
    if(op.isDefined)
      s"${op.get}:"
    else
      ""
  }

  override def toString = {
    val method = if(name == "<constructor>") "" else s".$name"
    val lv = localValName.map(x => s":$x") getOrElse ""
    s"${baseTrait.getSimpleName}${method}${lv} (parent:${parentValName.getOrElse(None)}) (L$line:$column)"
  }

  def refID: String = {

    s"${owner.getName}:${format(localValName)}${format(parentValName)}$name"
  }
}