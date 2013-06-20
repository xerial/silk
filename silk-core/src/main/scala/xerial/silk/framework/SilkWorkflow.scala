//--------------------------------------
//
// SilkWorkflow.scala
// Since: 2013/06/16 16:34
//
//--------------------------------------

package xerial.silk.framework

import scala.language.experimental.macros
import scala.reflect.macros.Context
import scala.reflect.ClassTag
import xerial.silk.framework
import java.util.UUID
import xerial.silk.framework.ops.{SilkMacros, SilkSeq, Silk}


private[silk] object WorkflowMacros {


  class WorkflowGen[C<:Context](val c:C) {

    def replaceDummy(tree:c.Tree, name:String) : c.Tree = {
      import c.universe._
      object replace extends Transformer {
        override def transform(tree: c.Tree) = {
          tree match {
            case id @ Ident(nme) if nme.decoded == "DummyWorkflow" =>
              Ident(newTypeName(name))
            case other => super.transform(tree)
          }
        }
      }
      val replaced = replace.transform(tree) // new A { val framework = ...; val session = ... }
      replaced
    }


  }



  def mixinImpl[A:c.WeakTypeTag](c:Context)(ev:c.Expr[ClassTag[A]]) = {
    import c.universe._
    val self = c.Expr[Class[_]](This(tpnme.EMPTY))
    val at = c.weakTypeOf[A]
    val t : c.Tree = reify {
      val w = self.splice.asInstanceOf[Workflow]
      new DummyWorkflow with Workflow {
        val env = w.env
      }
    }.tree

    val tree = new WorkflowGen[c.type](c).replaceDummy(t, at.typeSymbol.name.decoded).asInstanceOf[c.Tree]
    c.Expr[A](tree)
  }

  def newWorkflowImpl[A : c.WeakTypeTag](c:Context)(ev:c.Expr[ClassTag[A]]) = {
    import c.universe._
    val at = c.weakTypeOf[A]
    val t : c.Tree = reify {
      new DummyWorkflow with Workflow {
        val env = Workflow.defaultEnv
      }
    }.tree

    val tree = new WorkflowGen[c.type](c).replaceDummy(t, at.typeSymbol.name.decoded).asInstanceOf[c.Tree]
    c.Expr[A with Workflow](tree)
  }

}

case class SilkEnv(runner:SilkRunner, session:SilkSession) {
  def run[A](silk:Silk[A]) = runner.run(session, silk)
}

object Workflow {
  // TODO
  val defaultEnv : SilkEnv = null

  def of[A](implicit ev:ClassTag[A]) : A with Workflow = macro WorkflowMacros.newWorkflowImpl[A]

}

/**
 * Used in mixinImpl macro to find the code replacement target
 */
private[silk] trait DummyWorkflow extends Workflow {

}


trait Workflow extends Serializable {

  @transient val env : SilkEnv

  implicit class Runner[A](op:Silk[A]) {
    def run = env.run(op)
  }

  def newSilk[A](in:Seq[A])(implicit ev:ClassTag[A]): SilkSeq[A] = macro SilkMacros.newSilkImpl[A]

  /**
   * Import another workflow trait as a mixin to this class. The imported workflow shares the same session
   * @param ev
   * @tparam A
   * @return
   */
  def mixin[A](implicit ev:ClassTag[A]) : A = macro WorkflowMacros.mixinImpl[A]

}


