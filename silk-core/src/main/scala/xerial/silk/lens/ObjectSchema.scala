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
import tools.scalap.scalax.rules.scalasig._
import xerial.silk.util.Logger

//--------------------------------------
//
// ObjectSchema.scala
// Since: 2012/01/17 10:05
//
//--------------------------------------


/**
 * Object information extractor
 */
object ObjectSchema extends Logger {

  import java.{lang => jl}

  sealed trait Type {
    val name: String
  }

  abstract class ValueType(val rawType: Class[_]) extends Type {
    val name: String = rawType.getSimpleName

    override def toString = name
  }
  case class StandardType(override val rawType: Class[_]) extends ValueType(rawType)
  case class GenericType(override val rawType: Class[_], genericTypes: Seq[Type]) extends ValueType(rawType) {
    override def toString = "%s[%s]".format(rawType.getSimpleName, genericTypes.map(_.name).mkString(", "))
  }

  sealed abstract class Parameter(val name: String, val valueType: ValueType) {
    val rawType = valueType.rawType

    override def toString = "%s:%s".format(name, valueType)

    def findAnnotationOf[T <: jl.annotation.Annotation](implicit c: ClassManifest[T]): Option[T]

    protected def findAnnotationOf[T <: jl.annotation.Annotation](annot: Array[jl.annotation.Annotation])(implicit c: ClassManifest[T]): Option[T] = {
      annot.collectFirst {
        case x if (c.erasure isAssignableFrom x.annotationType) => x
      }.asInstanceOf[Option[T]]
    }
  }

  case class ConstructorParameter(owner: Class[_], index: Int, override val name: String, override val valueType: ValueType) extends Parameter(name, valueType) {
    def findAnnotationOf[T <: jl.annotation.Annotation](implicit c: ClassManifest[T]) = {
      val cc = owner.getConstructors()(0)
      val annot: Array[jl.annotation.Annotation] = cc.getParameterAnnotations()(index)
      findAnnotationOf[T](annot)
    }
  }

  case class FieldParameter(owner: Class[_], override val name: String, override val valueType: ValueType) extends Parameter(name, valueType) {
    lazy val field = owner.getField(name)

    def findAnnotationOf[T <: jl.annotation.Annotation](implicit c: ClassManifest[T]) = {
      owner.getDeclaredField(name) match {
        case null => None
        case field =>
          field.getAnnotation[T](c.erasure.asInstanceOf[Class[T]]) match {
            case null => None
            case a => Some(a.asInstanceOf[T])
          }
      }
    }
  }

  case class MethodParameter(owner: jl.reflect.Method, index: Int, override val name: String, override val valueType: ValueType) extends Parameter(name, valueType) {
    def findAnnotationOf[T <: jl.annotation.Annotation](implicit c: ClassManifest[T]): Option[T] = {
      val annot: Array[jl.annotation.Annotation] = owner.getParameterAnnotations()(index)
      findAnnotationOf[T](annot)
    }
  }


  case class Method(owner: Class[_], name: String, argTypes: Array[MethodParameter], returnType: Type) extends Type {
    override def toString = "Method(%s#%s, [%s], %s)".format(owner.getSimpleName, name, argTypes.mkString(", "), returnType)

    lazy val jMethod = owner.getMethod(name, argTypes.map(_.rawType): _*)

    def findAnnotationOf[T <: jl.annotation.Annotation](implicit c: ClassManifest[T]): Option[T] = {
      jMethod.getAnnotation(c.erasure.asInstanceOf[Class[T]]) match {
        case null => None
        case a => Some(a.asInstanceOf[T])
      }
    }
    def findAnnotationOf[T <: jl.annotation.Annotation](paramIndex: Int)(implicit c: ClassManifest[T]): Option[T] = {
      argTypes(paramIndex).findAnnotationOf[T]
    }

  }

  case class Constructor(cl: Class[_], params: Array[ConstructorParameter]) extends Type {
    val name = cl.getSimpleName
    override def toString = "Constructor(%s, [%s])".format(cl.getSimpleName, params.mkString(", "))
  }


  implicit def toSchema(cl: Class[_]): ObjectSchema = ObjectSchema(cl)

  private val schemaTable = new WeakHashMap[Class[_], ObjectSchema]

  /**
   * Get the object schema of the specified type. This method caches previously created ObjectSchema instances, and
   * second call for the same type object return the cached entry.
   */
  def apply(cl: Class[_]): ObjectSchema = schemaTable.getOrElseUpdate(cl, new ObjectSchema(cl))

  def getSchemaOf(obj: Any): ObjectSchema = apply(obj.getClass)


  def findSignature(cl: Class[_]): Option[ScalaSig] = {
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

    val sig = ScalaSigParser.parse(cl)

    sig match {
      case Some(x) => sig
      case None => {
        enclosingObject(cl) match {
          case Some(x) => findSignature(x)
          case None => None
        }
      }
    }
  }

  private def findConstructor(cl:Class[_], sig:ScalaSig) : Option[Constructor] = {
    import scala.tools.scalap.scalax.rules.scalasig
    def isTargetClass(t: scalasig.Type): Boolean = {
      t match {
        case TypeRefType(_, ClassSymbol(sinfo, _), _) => {
          //debug("className = %s, found name = %s", className, sinfo.name)
          sinfo.name == cl.getSimpleName
        }
        case _ => false
      }
    }
    def findConstructorParameters(mt: MethodType, sig: ScalaSig): Array[ConstructorParameter] = {
      val paramSymbols: Seq[MethodSymbol] = mt match {
        case MethodType(_, param: Seq[_]) => param.collect {
          case m: MethodSymbol => m
        }
        case _ => Seq.empty
      }

      val l = for (((name, vt), index) <- toAttribute(paramSymbols, sig).zipWithIndex) yield ConstructorParameter(cl, index, name, vt)
      l.toArray
    }

    val entries = (0 until sig.table.length).map(sig.parseEntry(_))
    entries.collectFirst {
      case m: MethodType if isTargetClass(m.resultType) =>
        Constructor(cl, findConstructorParameters(m, sig))
    }
  }

  private def findConstructor(cl: Class[_]): Option[Constructor] = {
    findSignature(cl) match {
      case Some(sig) => findConstructor(cl, sig)
      case None => None
    }
  }

  private def toAttribute(param: Seq[MethodSymbol], sig: ScalaSig): Seq[(String, ValueType)] = {
    val paramRefs = param.map(p => (p.name, sig.parseEntry(p.symbolInfo.info)))
    val paramSigs = paramRefs.map {
      case (name: String, t: TypeRefType) => (name, t)
    }

    for ((name, typeSignature) <- paramSigs) yield (name, resolveType(typeSignature))
  }

  def isOwnedByTargetClass(m: MethodSymbol, cl: Class[_]): Boolean = {
    m.symbolInfo.owner match {
      case ClassSymbol(symbolInfo, _) => symbolInfo.name == cl.getSimpleName
      case _ => false
    }
  }

  def parametersOf(cl: Class[_]): Array[Parameter] = {
    findSignature(cl) match {
      case None => Array.empty
      case Some(sig) => {
        val entries = (0 until sig.table.length).map(sig.parseEntry(_))

        val constructorParams = findConstructor(cl, sig) match {
          case None => Array.empty
          case Some(cc) => cc.params
        }


        def isFieldReader(m:MethodSymbol) : Boolean = {
          m.isAccessor && !m.isParamAccessor && isOwnedByTargetClass(m, cl) && !m.name.endsWith("_$eq")
        }

        val fieldParams = entries.collect {
          case m: MethodSymbol if isFieldReader(m) => {
            entries(m.symbolInfo.info) match {
              case NullaryMethodType(resultType: TypeRefType) =>
                FieldParameter(cl, m.name, resolveType(resultType))
            }
          }
        }

        (constructorParams.toSeq ++ fieldParams).toArray
      }
    }
  }

  def methodsOf(cl: Class[_]): Array[Method] = {
    findSignature(cl) match {
      case None => Array.empty
      case Some(sig) => {
        val entries = (0 until sig.table.length).map(sig.parseEntry(_))

        def isTargetMethod(m: MethodSymbol): Boolean = {
          m.isMethod && !m.isAccessor && m.name != "<init>" && isOwnedByTargetClass(m, cl)
        }

        val methods = entries.collect {
          case m: MethodSymbol if isTargetMethod(m) => {
            val methodType = entries(m.symbolInfo.info)
            methodType match {
              case NullaryMethodType(resultType: TypeRefType) => {
                Method(cl, m.name, Array.empty[MethodParameter], resolveType(resultType))
              }
              case MethodType(resultType: TypeRefType, paramSymbols: Seq[_]) => {
                val params = toAttribute(paramSymbols.asInstanceOf[Seq[MethodSymbol]], sig)
                val jMethod = cl.getMethod(m.name, params.map(_._2.rawType): _*)
                val mp = for (((name, vt), index) <- params.zipWithIndex) yield MethodParameter(jMethod, index, name, vt)
                Method(cl, m.name, mp.toArray, resolveType(resultType))
              }
            }
          }
        }
        methods.toArray
      }
    }
  }

  def resolveType(typeSignature: TypeRefType): ValueType = {
    val name = typeSignature.symbol.toString()
    val clazz: Class[_] = {
      name match {
        // Resolve primitive types.
        // This specital treatment is necessary because scala.Int etc. classes do exists but,
        // Scala compiler reduces AnyVal types (e.g., classOf[Int] etc) into Java primitive types (e.g., int, float)
        case "scala.Boolean" => classOf[Boolean]
        case "scala.Byte" => classOf[Byte]
        case "scala.Short" => classOf[Short]
        case "scala.Char" => classOf[Char]
        case "scala.Int" => classOf[Int]
        case "scala.Float" => classOf[Float]
        case "scala.Long" => classOf[Long]
        case "scala.Double" => classOf[Double]
        case "scala.Predef.String" => classOf[String]
        case "scala.Predef.Map" => classOf[Map[_, _]]
        case "scala.package.Seq" => classOf[Seq[_]]
        case _ =>
          try Class.forName(name)
          catch {
            case _ => throw new IllegalArgumentException("unknown type: " + name)
          }
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

}


/**
 * Contains information of methods, constructor and parameters defined in a class
 * @author leo
 */
class ObjectSchema(val cl: Class[_]) extends Logger {

  import ObjectSchema._

  val name: String = cl.getSimpleName
  val fullName: String = cl.getName

  def findSignature: Option[ScalaSig] = ObjectSchema.findSignature(cl)

  lazy val parameters: Array[Parameter] = parametersOf(cl)
  lazy val methods: Array[Method] = methodsOf(cl)

  lazy private val parameterIndex: Map[String, Parameter] = {
    val pair = for (a <- parameters) yield a.name -> a
    pair.toMap
  }

  def getParameter(name: String): Parameter = {
    parameterIndex(name)
  }


  lazy val constructor: Constructor = {
    findConstructor(cl) match {
      case Some(c) => c
      case None => throw new IllegalArgumentException("no constructor is found for " + cl)
    }
  }

  override def toString = "%s(%s)".format(cl.getSimpleName, parameters.mkString(", "))


}

