//--------------------------------------
//
// SilkWorkflow.scala
// Since: 2013/06/16 16:34
//
//--------------------------------------

package xerial.silk.framework

import scala.reflect.macros.Context
import scala.reflect.ClassTag
import xerial.silk.framework.ops.SilkOps
import xerial.silk.framework


object Workflow {

  def mixinImpl[A:c.WeakTypeTag](c:Context)(ev:c.Expr[ClassTag[A]]) = {
    import c.universe._
    val self = c.Expr[Class[_]](This(tpnme.EMPTY))
    val at = c.weakTypeOf[A]
    val t : c.Tree = reify {
      new WithSession(self.splice.asInstanceOf[Workflow].session) with DummyWorkflow
    }.tree

    object replace extends Transformer {
      override def transform(tree: Tree) = {
        tree match {
          case id @ Ident(nme) if nme.decoded == "DummyWorkflow" =>
            Ident(newTypeName(at.typeSymbol.name.decoded))
          case other => super.transform(tree)
        }
      }
    }
    val replaced = replace.transform(t)
    c.Expr[A](replaced)
  }

}

/**
 * Used in mixinImpl macro to find the code replacement target
 */
private[silk] trait DummyWorkflow { this:Workflow =>

}



trait Workflow extends Serializable {
  self: SessionManagerComponent =>

  @transient implicit val session : Session

  implicit class Runner[A](op:SilkOps[A]) {
    def run = session.run(op)
  }

  /**
   * Import another workflow trait as a mixin to this class. The imported workflow shares the same session
   * @param ev
   * @tparam A
   * @return
   */
  def mixin[A](implicit ev:ClassTag[A]) : A = macro Workflow.mixinImpl[A]

}


//class MyWorkflow extends Workflow {
//
//  //@transient implicit val session =
//}

abstract class WithSession extends Workflow with SessionManagerComponent {
  val session : Session
}
