/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package xerial.silk.core

import java.io.{File, PrintWriter, StringWriter}

import xerial.lens.ObjectSchema
import xerial.silk.SqlContext
import scala.language.experimental.macros
import SilkMacros._
import xerial.silk._

/**
 *
 */
trait Frame[A] {

  def context: FContext
  def inputs : Seq[Frame[_]]
  def summary : String

  def name = this.getClass.getSimpleName
  override def toString = new FrameFormatter().format(this).result

  def limit(rows:Int) : Frame[A] = macro mLimit[A]
  def limit(rows:Int, offset:Int) : Frame[A] = macro mLimitWithOffset[A]

  def select1 : Option[Single[A]] = null
  def select(cols: (A => Column[A, _])*) : Frame[A] = macro mSelect[A]
  def selectAll : Frame[A] = null
  def filter(condition:A => Cond[A]) : Frame[A] = macro mFilter[A]

  def orderBy() : Frame[A] = null

  def size: Single[Int] = null

  def print = null

  def between(from:Schedule, to:Schedule) : Frame[A] = null

  def as[B] : Frame[B] = macro mAs[B]

  //def run(implicit executor:Executor) = null

  def dependsOn(others:Frame[A]*) : Frame[A] = {
    val sc = ObjectSchema(this.getClass)
    val constructorArgs = sc.constructor.params
    val hasInputsColumn = constructorArgs.find(_.name == "inputs").isDefined
    val params = for(p <- constructorArgs) yield {
      val newDependencies = this.inputs ++ others
      val newV = if(p.name == "inputs") {
        newDependencies
      }
      else {
        p.get(this)
      }
      newV.asInstanceOf[AnyRef]
    }
    if(hasInputsColumn) {
      val c = sc.constructor.newInstance(params.toSeq.toArray[AnyRef])
      c.asInstanceOf[this.type]
    }
    else {
      Knot[A](context, this.inputs ++ others, this)
    }
  }

  def unionAll(other:Frame[A]) : Frame[A] = NA
  def union(other:Frame[A]) : Frame[A] = NA
  def merge(other:Frame[A]) : Frame[A] = NA


  // numeric operations
  def sum[C](col: A => Column[A, C])(implicit tpe: Numeric[C]) : Single[Int] = NA

  /**
   * Generates non-overlapping windows of a fixed window
   * @param windowSize
   */
  def fixedDuration(windowSize:Duration) : Frame[A] = NA

  /**
   * Generates sliding windows that allows overlaps and having a given size of gap (step) between each window
   * @param windowSize
   * @param step
   * @return
   */
  def sliding(windowSize:Duration, step:Duration) : Frame[A] = NA

  def fixedSize(numItem:Int) : Frame[A] = NA


  def aggregate(col:(A => Column[_, _])*) : Frame[A] = NA
  def groupBy(col:(A=>Column[A, _])*) : Frame[_] = NA
}




object Frame {

}

class FrameFormatter {

  private val buf = new StringWriter()
  private val out : PrintWriter = new PrintWriter(buf)


  private def indent(indentLevel:Int) : String = {
    (0 until indentLevel).map(_ => " ").mkString
  }

  def format(frame:Frame[_], indentLevel:Int = 0): FrameFormatter = {
    if(frame != null) {
      out.println(s"${indent(indentLevel)}[${frame.name}] ${frame.summary}")
      if(frame.context != FContext.empty) {
        out.println(s"${indent(indentLevel+1)}context: ${frame.context}")
      }
      if(!frame.inputs.isEmpty) {
        out.println(s"${indent(indentLevel+1)}inputs:")
      }
      for (in <- frame.inputs.seq) {
        format(in, indentLevel + 2)
      }
    }
    this
  }

  def result : String = {
    out.flush()
    buf.toString
  }
}

trait RootFrame[A] extends Frame[A] {
  def context = FContext.empty
  def inputs = Seq.empty
  def summary = ""
}

case class Knot[A](context:FContext, inputs:Seq[Frame[_]], output:Frame[A]) extends Frame[A]  {
  def summary = output.summary
  override def name = output.name
}

case class InputFrame[A](context:FContext, data:Seq[A]) extends Frame[A] {
  def inputs = Seq.empty
  def summary = data.toString
}

case class FileInput[A](context:FContext, file:File) extends Frame[A] {
  def inputs = Seq.empty
  def summary = s"file: ${file.getPath}"
}

case class FrameRef[A](context:FContext) extends Frame[A] {
  def inputs = Seq.empty
  def summary = "frame ref"
}

case class RawSQL(context:FContext, sc:SqlContext, args:Seq[Any]) extends Frame[Any] {
  def inputs = args.collect{case f:Frame[_] => f}
  def summary = templateString(sc.sc)

  private def templateString(sc:StringContext) = {
    sc.parts.mkString("{}")
  }
}






case class CastAs[A](context:FContext, input:Frame[_]) extends Frame[A] {
  def inputs = Seq(input)
  def summary = ""
}

import SilkException._

/**
 *
 */
case class Column[Table, ColType](name:String)
{
  // TODO
  def is[A](other:A) : Cond[Table] = NA
  def >(v:Int) : Cond[Table] = NA
  def >=(v:Int) : Cond[Table] = NA
  def <(v:Int) : Cond[Table] = NA
  def <=(v:Int) : Cond[Table] = NA

  def as(newAlias:String) : Column[Table, ColType] = null

  // Column aggregation operation
  def min : Column[_, Int] = NA
  def max : Column[_, Int] = NA
  def avg : Column[_, Int] = NA
  def sum : Column[_, Int] = NA


}

trait Single[Int]

trait Cond[A]
//case class Eq[A](other:Col[_]) extends Cond[A]
//case class EqExpr[A](cond:Col[A] => Boolean) extends Cond[A]

case class LimitOp[A](context:FContext, input:Frame[A], rows:Int, offset:Int) extends Frame[A] {
  def inputs = Seq(input)
  def summary = s"rows:${rows}, offset:${offset}"
}
case class FilterOp[A](context:FContext, input:Frame[A], cond:A => Cond[A]) extends Frame[A] {
  def inputs = Seq(input)
  def summary = s"condition: ${cond}"
}
case class ProjectOp[A](context:FContext, input:Frame[A], col:Seq[A => Column[A, _]]) extends Frame[A] {
  def inputs = Seq(input)
  def summary = "select"
}

object SQLHelper {

  def templateString(sc:StringContext) = {
    val b = new StringBuilder
    for(p <- sc.parts) {
      b.append(p)
      b.append("${}")
    }
    b.result()
  }


  private def isFrameType[A](cl: Class[A]): Boolean = classOf[Frame[_]].isAssignableFrom(cl)

  def frameInputs(args:Seq[Any]) = {
    val b = Seq.newBuilder[Frame[_]]
    for((a, index) <- args.zipWithIndex) {
      if(isFrameType(a.getClass)) {
        b += a.asInstanceOf[Frame[_]]
      }
    }
    b.result
  }

}

