//--------------------------------------
//
// SilkWorkflow.scala
// Since: 2013/06/16 16:34
//
//--------------------------------------

package xerial.silk.core

import scala.language.experimental.macros
import scala.reflect.macros.Context
import scala.reflect.ClassTag
import xerial.silk.framework.core.{FContext, SilkMacros}


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
    val helper = new SilkMacros.MacroHelper(c)
    val _fc = helper.createFContext

    val t : c.Tree = reify {
      new DummyWorkflow with Workflow {
        val fc = _fc.splice
      }
    }.tree

    val tree = new WorkflowGen[c.type](c).replaceDummy(t, at.typeSymbol.name.decoded).asInstanceOf[c.Tree]
    c.Expr[A with Workflow](tree)
  }

  def newWorkflowImpl[A : c.WeakTypeTag](c:Context)(ev:c.Expr[ClassTag[A]]) = {
    import c.universe._
    val at = c.weakTypeOf[A]
    val helper = new SilkMacros.MacroHelper(c)
    val _fc = helper.createFContext

    val t : c.Tree = reify {
      new DummyWorkflow with Workflow {
        val fc =  _fc.splice
      }
    }.tree

    val tree = new WorkflowGen[c.type](c).replaceDummy(t, at.typeSymbol.name.decoded).asInstanceOf[c.Tree]
    c.Expr[A with Workflow](tree)
  }

}


/**
 * Used in mixinImpl macro to find the code replacement target
 */
private[silk] trait DummyWorkflow {

}

/**
 * Workflow trait with FContext
 */
trait Workflow extends Serializable {
  val fc : FContext
}


