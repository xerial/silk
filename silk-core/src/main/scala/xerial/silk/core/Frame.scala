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
import java.util.Properties

import org.joda.time.DateTime
import xerial.lens.ObjectSchema
import xerial.silk._
import xerial.silk.macros.SilkMacros._

import scala.reflect.ClassTag

/**
 *
 */
object Frame {


}

/**
 *
 */
trait Frame[A] extends Task {
  override def toString = new FrameFormatter().graphFormat(this).result

  def limit(rows: Int): Frame[A] = limit(rows, 0)
  def limit(rows: Int, offset: Int): Frame[A] = LimitOp(context, this, rows, offset)

  def select1: Option[Single[A]] = NA
  def select(cols: (A => Column[_])*): Frame[A] = ProjectOp(context, this, cols)
  def selectAll : Frame[A] = NA
  def filter(condition: A => Cond): Frame[A] = FilterOp(context, this, condition)

  def mapColumn[C, D](col:A => Column[C], expr: String) : Frame[_] = NA

  def orderBy(cols: (A => Column[_])*): Frame[A] = null

  def size: Single[Int] = null

  def print = null

  def between(from: Schedule, to: Schedule): Frame[A] = null

  def as[B:ClassTag]: Frame[B] = CastAs[A, B](context, this)

  //def run(implicit executor:Executor) = null


  def unionAll(other: Frame[A]): Frame[A] = NA
  def union(other: Frame[A]): Frame[A] = NA
  def merge(other: Frame[A]): Frame[A] = NA


  // numeric operations
  def sum[C](col: A => Column[C])(implicit tpe: Numeric[C]): Single[Int] = NA

  /**
   * Generates non-overlapping windows of a fixed window
   * @param windowSize
   */
  def fixedDuration(windowSize: Duration): Frame[A] = NA

  /**
   * Generates sliding windows that allows overlaps and having a given size of gap (step) between each window
   * @param windowSize
   * @param step
   * @return
   */
  def sliding(windowSize: Duration, step: Duration): Frame[A] = NA

  def fixedSize(numItem: Int): Frame[A] = NA


  def aggregate(col: (A => Column[_])*): Frame[A] = NA
  def groupBy(col: (A => Column[_])*): Frame[_] = NA


  def partitionBy[K](col: A => Column[K]) : FrameWindow[K, A] = NA
  def partitionBy[K1, K2](col1: A => Column[K1], col2: A => Column[K2]) : FrameWindow[(K1, K2), A] = NA

  def exportTo(target:String) = NA

}

trait ExportTarget


trait WindowOp[C]

case class FirstInWindow[C](col:Column[C]) extends WindowOp[C]
case class NthInWindow[C](col:Column[C], offset:Int) extends WindowOp[C]
case class LastInWindow[C](col:Column[C]) extends WindowOp[C]
case class LagInWindow[C](col:Column[C], offset:Int, defaultValue:C) extends WindowOp[C]
case class LeadInWindow[C](col:Column[C], offset:Int, defaultValue:C) extends WindowOp[C]

class FrameWindow[K, A](table:A) {

  def orderBy(cols: (A => Column[K])*) : FrameWindow[K, A] = NA

  def select[C](cols: (A => WindowOp[C])*) : Frame[A] = NA

  def firstsInWindows : Frame[A] = NA
  def lastsInWindows : Frame[A] = NA

}


trait TDFrame[A] extends Frame[A] {

  def timeRange(from:DateTime, to:DateTime) : Frame[A] = NA


}


class FrameFormatter {

  private val buf = new StringWriter()
  private val out: PrintWriter = new PrintWriter(buf)
  private var printed : Set[Task] = Set.empty

  private def indent(indentLevel: Int): String = {
    (0 until indentLevel).map(_ => " ").mkString
  }

  def graphFormat[A](frame:Task) : FrameFormatter = {
    if(frame != null) {

    }



    this
  }

  def format[A](frame: Task, indentLevel: Int = 0): FrameFormatter = {
    if (frame != null) {
      if(printed.contains(frame)) {
        out.println(s"${indent(indentLevel)} *${frame.summary}")
      }
      else {
        printed += frame
        out.println(s"${indent(indentLevel)}[${frame.name}] ${frame.summary}")
        //out.println(s"${indent(indentLevel + 1)}context: ${frame.context}")
        val inputs = frame.context.inputs
        if (!inputs.isEmpty) {
          out.println(s"${indent(indentLevel + 1)}inputs:")
        }
        for (in <- inputs.seq) {
          format(in, indentLevel + 2)
        }
      }
    }
    this
  }

  def result: String = {
    out.flush()
    buf.toString
  }
}

case class RootFrame[A](context:TaskContext) extends Frame[A] {
  def summary = ""
}

case class MultipleInputs(context:TaskContext) extends Task {
  def summary = s"${context.inputs.size} inputs"
  override def name = s"MultipleInputs"
}

case class InputFrame[A](context:TaskContext, data: Seq[A]) extends Frame[A] {
  def summary = data.toString
}

case class FileInput(context:TaskContext, file: File) extends Task {
  def summary = s"file: ${file.getPath}"
}

case class FrameRef[A](context:TaskContext) extends Frame[A] {
  def summary = "frame ref"
}

case class CastAs[A, B](context:TaskContext, input:Frame[A])(implicit c:ClassTag[B]) extends Frame[B] {
  def summary = s"cast as ${c}"
}

trait Schema


/**
 *
 */
case class Column[ColType](name: String) {
  // TODO
  def is[A](other: A): Cond = NA
  def in(includes: ColType*): Cond = NA
  def >(v: Int): Cond = NA
  def >=(v: Int): Cond = NA
  def <(v: Int): Cond = NA
  def <=(v: Int): Cond = NA

  def as(newAlias: String): Column[ColType] = null

  // Column aggregation operation
  def min: Column[Int] = NA
  def max: Column[Int] = NA
  def avg: Column[Int] = NA
  def sum: Column[Int] = NA

  // window op
  def first : FirstInWindow[ColType] = NA
  def last : LastInWindow[ColType] = NA
  def nth(offset:Int) : NthInWindow[ColType] = NA
  def lag(offset:Int) : LagInWindow[ColType] = NA
  def lead(offset:Int) : LeadInWindow[ColType] = NA

}

trait Single[Int]

trait Cond

//case class Eq[A](other:Col[_]) extends Cond[A]
//case class EqExpr[A](cond:Col[A] => Boolean) extends Cond[A]

case class LimitOp[A](context:TaskContext, input: Frame[A], rows: Int, offset: Int) extends Frame[A] {
  def summary = s"rows:${rows}, offset:${offset}"
}

case class FilterOp[A](context:TaskContext, input: Frame[A], cond: A => Cond) extends Frame[A] {
  def summary = s"condition: ${cond}"
}

case class ProjectOp[A](context:TaskContext, input: Frame[A], col: Seq[A => Column[_]]) extends Frame[A] {
  def summary = "select"
}

case class SelectAll(context:TaskContext, table:TableRef) extends Frame[Any] {
  def summary = "selectAll"
}

case class SQLOp(context:TaskContext, db: Database, sql: String) extends Frame[Any] {
  def summary = s"sql: ${sql}"
}

trait Database {
  def name : String

  def sql(sql: String): SQLOp = macro mSQL
  def query(sql: String): SQLOp = macro mSQL

  def table(name: String): TableRef = macro mTableOpen
  def createTable(name:String, colDef:String) : TableRef = macro mTableCreate
  def createTableIfNotExists(name:String, colDef:String) : TableRef = macro mTableCreateIfNotExists
  def dropTable(name:String) : DropTable = macro mTableDrop
  def dropTableIfExists(name:String) : DropTableIfExists = macro mTableDropIfExists

  def insertInto(table:TableRef, frame:Frame[_]) : Frame[_] = NA
}


abstract class TableRef(val context:TaskContext, val db: Database, val tableName: String) extends Frame[Any] {
  override def summary: String = s"${db.name}.${tableName}"

  //def selectAll: SelectAll = macro mSelectAll
}

case class OpenTable(override val context:TaskContext, override val db:Database, override val tableName:String) extends TableRef(context, db, tableName) {
  override def summary = s"open table ${db.name}.${tableName}"
}
case class CreateTable(override val context:TaskContext, override val db:Database, override val tableName:String, colDef:String) extends TableRef(context, db, tableName) {
  override def summary = s"create table ${db.name}.${tableName}"
}
case class CreateTableIfNotExists(override val context:TaskContext, override val db:Database, override val tableName:String, colDef:String) extends TableRef(context, db, tableName) {
  override def summary = s"create table if not exists ${db.name}.${tableName}"
}
case class DropTable(override val context:TaskContext, db:Database, tableName:String) extends Task {
  override def summary = s"drop table ${db.name}.${tableName}"
}
case class DropTableIfExists(override val context:TaskContext, db:Database, tableName:String) extends Task {
  override def summary = s"drop table if exists ${db.name}.${tableName}"
}


object SQLHelper {

  def templateString(sc: StringContext) = {
    val b = new StringBuilder
    for (p <- sc.parts) {
      b.append(p)
      b.append("${}")
    }
    b.result()
  }

  private def isFrameType[A](cl: Class[A]): Boolean = classOf[Frame[_]].isAssignableFrom(cl)

  def frameInputs(args: Seq[Any]) = {
    val b = Seq.newBuilder[Frame[_]]
    for ((a, index) <- args.zipWithIndex) {
      if (isFrameType(a.getClass)) {
        b += a.asInstanceOf[Frame[_]]
      }
    }
    b.result
  }

}

