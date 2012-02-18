/*
 * Copyright 2012 Taro L. Saito
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xerial.silk.lens

import collection.mutable.WeakHashMap
import java.lang.reflect.Field
import reflect.ScalaSignature
import xerial.silk.util.{Logging, StringTemplate, TypeUtil}
import tools.scalap.scalax.rules.scalasig._

//--------------------------------------
//
// ObjectSchema.scala
// Since: 2012/01/17 10:05
//
//--------------------------------------

/**
 *
 */
object ObjectSchema {

  private val schemaTable = new WeakHashMap[Class[_], ObjectSchema]

  def getSchemaOf(obj: Any): ObjectSchema = apply(obj.getClass)

  /**
   * Get the object schema of the specified type. This method caches previously created ObjectSchema instances, and
   * second call for the same type object return the cached entry.
   */
  def apply(cl: Class[_]): ObjectSchema = schemaTable.getOrElseUpdate(cl, new ObjectSchema(cl))


  def detectInterfaceSignature(cl: Class[_]) = {
    val interfaces = {
      val f = cl.getInterfaces
      if (f == null) Array() else f
    }
    def isClassOfInterest(c: Class[_]) = {
      val systemPackagePrefix = Array("java.", "javax.", "scala.")
      if (systemPackagePrefix.find(c.getName.startsWith(_)).isDefined)
        false
      else {
        if (c.getEnclosingClass != null)
          false
        else
          true
      }
    }

    for (each <- interfaces; if isClassOfInterest(each)) yield ScalaSigParser.parse(each)
  }

  def detectSignature(cl: Class[_]) = {
    val sig = ScalaSigParser.parse(cl)
    sig
  }


  abstract class Type(val rawType: Class[_]) {
    def getSimpleName: String = rawType.getSimpleName
  }

  case class StandardType(valueType: Class[_]) extends Type(valueType) {
    override def toString = getSimpleName
  }

  case class GenericType(valueType: Class[_], genericTypes: Seq[Type]) extends Type(valueType) {
    override def toString = "%s[%s]".format(valueType.getSimpleName, genericTypes.map(_.getSimpleName).mkString(", "))
  }

  case class Attribute(name: String, valueType: Type) {
    val rawType = valueType.rawType

    override def toString = "%s:%s".format(name, valueType)
  }


}


/**
 * Information of object parameters and their s
 * @author leo
 */
class ObjectSchema(val cl: Class[_]) {

  import ObjectSchema._

  private def lookupAttributes: Array[Attribute] = {
    // filter internal scala fields
    val v = Array.newBuilder[Attribute]
    for (f <- cl.getDeclaredFields; if !f.getName.startsWith("$")) {
      v += Attribute(f.getName, StandardType(f.getType))
    }
    v.result
  }

  val name: String = cl.getSimpleName
  val fullName: String = cl.getName
  val attributes: Array[Attribute] = lookupAttributes
  private val attributeIndex: Map[String, Attribute] = {
    val pair = for (a <- attributes) yield a.name -> a
    pair.toMap
  }

  def getAttribute(name: String): Attribute = {
    attributeIndex(name)
  }

  /**
   * Read the object parameter by using reflection
   */
  def read(obj: Any, attribute: Attribute): Any = {
    val f: Field = obj.getClass.getDeclaredField(attribute.name)
    val v = TypeUtil.readField(obj, f)
    v
  }

  override def toString = {
    val b = new StringBuilder
    b append (cl.getSimpleName + "(")
    b append (attributes.mkString(", "))
    b.append(")")
    b.toString
  }
}


object ScalaClassLens extends Logging {

  def enclosingObject(cl: Class[_]): Option[Class[_]] = {
    val pos = cl.getName.lastIndexOf("$")
    val parentClassName = cl.getName.slice(0, pos)
    try {
      Some(Class.forName(parentClassName))
    }
    catch {
      case e => None
    }
  }


  def detectSignature(cl: Class[_]): Option[ScalaSig] = {
    val sig = ScalaSigParser.parse(cl)

    sig match {
      case Some(x) => sig
      case None => {
        enclosingObject(cl) match {
          case Some(x) => detectSignature(x)
          case None => None
        }
      }
    }
  }

  import ObjectSchema._
  import scala.tools.scalap.scalax.rules.scalasig

  def getConstructor(cl: Class[_]): MethodType = {
    val cc = findConstructor(cl)
    if (cc.isEmpty)
      throw new IllegalArgumentException("no constructor is found for " + cl)
    else
      cc.get
  }

  def findConstructor(cl: Class[_]): Option[MethodType] = {
    val sig = detectSignature(cl).get
    findConstructor(cl, sig)
  }

  def findConstructor(cl: Class[_], sig: ScalaSig): Option[MethodType] = {
    val className = cl.getSimpleName
    val entries = (0 until sig.table.length).map(sig.parseEntry(_))

    def isTargetClass(t: scalasig.Type): Boolean = {
      t match {
        case TypeRefType(_, ClassSymbol(sinfo, _), _) => {
          //debug("className = %s, found name = %s", className, sinfo.name)
          sinfo.name == className
        }
        case _ => false
      }
    }
    entries.collectFirst {
      case m: MethodType if isTargetClass(m.resultType) => m
    }
  }

  def findConstructorParameters(cl: Class[_]): Array[Attribute] = {
    val sig = detectSignature(cl).get
    val paramSymbols: Seq[MethodSymbol] = findConstructor(cl, sig) match {
      case Some(MethodType(_, param: Seq[_])) => param.collect {
        case m: MethodSymbol => m
      }
      case _ => Seq.empty
    }

    val paramRefs = paramSymbols.map(p => (p.name, sig.parseEntry(p.symbolInfo.info)))
    val paramSigs = paramRefs.map {
      case (name: String, t: TypeRefType) => (name, t)
    }

    val b = Array.newBuilder[Attribute]
    for ((name, typeSignature) <- paramSigs) {
      val t = resolveType(typeSignature)
      b += new Attribute(name, t)
    }
    b.result
  }

  def findParameters(cl: Class[_]): Array[Attribute] = {
    val sig = detectSignature(cl).get
    debug {
      sig
    }
    val entries = (0 until sig.table.length).map(sig.parseEntry(_))

    val paramTypes = entries.collect {
      case m: MethodSymbol if m.isAccessor => {
        entries(m.symbolInfo.info) match {
          case NullaryMethodType(resultType: TypeRefType) => (m.name, resolveType(resultType))
        }
      }
    }


    debug {
      paramTypes.mkString("\n")
    }

    val b = Array.newBuilder[Attribute]

    b.result
  }

  def resolveType(typeSignature: TypeRefType): Type = {
    val name = typeSignature.symbol.toString()
    val clazz: Class[_] = {
      try {
        Class.forName(name)
      }
      catch {
        case _ => resolveClass(name)
      }
    }

    if (typeSignature.typeArgs.isEmpty) {
      StandardType(clazz)
    }
    else {
      val typeArgs: Seq[Type] = typeSignature.typeArgs.map {
        case x: TypeRefType => resolveType(x)
      }
      GenericType(clazz, typeArgs)
    }
  }

  def resolveClass(name: String): Class[_] = {

    name match {

      case p if p.startsWith("scala.Predef") => {
        p match {
          case t if t.startsWith("scala.Predef.String") => classOf[String]
          case t if t.startsWith("scala.Predef.Map") => classOf[Map[_, _]]
          case t if t.startsWith("scala.package.Seq") => classOf[Seq[_]]
          case _ => throw new IllegalArgumentException("unknown type: " + name)
        }
      }
      case p if p.startsWith("scala.package") => {
        p match {
          case t if t.startsWith("scala.package.Seq") => classOf[Seq[_]]
          case _ => throw new IllegalArgumentException("unknown type: " + name)
        }
      }
      case _ => throw new IllegalArgumentException("unknown type: " + name)
    }

  }

}

