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
import xerial.silk.util.{TypeUtil, Logger}

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
    def isOption = false
    def isBooleanType = false
    def isGenericType = false
  }

  case class StandardType(override val rawType: Class[_]) extends ValueType(rawType) {
    override def isBooleanType = rawType == classOf[Boolean]
  }

  case class GenericType(override val rawType: Class[_], genericTypes: Seq[ValueType]) extends ValueType(rawType) {
    override def toString = "%s[%s]".format(rawType.getSimpleName, genericTypes.map(_.name).mkString(", "))

    override def isOption = rawType == classOf[Option[_]]
    override def isBooleanType = isOption && genericTypes(0).isBooleanType
    override def isGenericType = true
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

    def get(obj: Any): Any
  }

  case class ConstructorParameter(owner: Class[_], fieldOwner: Class[_], index: Int, override val name: String, override val valueType: ValueType) extends Parameter(name, valueType) {
    lazy val field = fieldOwner.getDeclaredField(name)
    def findAnnotationOf[T <: jl.annotation.Annotation](implicit c: ClassManifest[T]) = {
      val cc = owner.getConstructors()(0)
      val annot: Array[jl.annotation.Annotation] = cc.getParameterAnnotations()(index)
      findAnnotationOf[T](annot)
    }

    def get(obj: Any) = {
      TypeUtil.readField(obj, field)
    }

  }

  case class FieldParameter(owner: Class[_], ref: Class[_], override val name: String, override val valueType: ValueType) extends Parameter(name, valueType) with Logger {
    lazy val field = {
      try
        owner.getDeclaredField(name)
      catch {
        case _ =>
          warn("no such field %s in %s (ref:%s)", name, owner.getSimpleName, ref.getSimpleName)
          null
      }
    }

    def findAnnotationOf[T <: jl.annotation.Annotation](implicit c: ClassManifest[T]) = {
      field match {
        case null => None
        case field =>
          field.getAnnotation[T](c.erasure.asInstanceOf[Class[T]]) match {
            case null => None
            case a => Some(a.asInstanceOf[T])
          }
      }
    }

    def get(obj: Any) = {
      TypeUtil.readField(obj, field)
    }
  }

  case class MethodParameter(owner: jl.reflect.Method, index: Int, override val name: String, override val valueType: ValueType) extends Parameter(name, valueType) {
    def findAnnotationOf[T <: jl.annotation.Annotation](implicit c: ClassManifest[T]): Option[T] = {
      val annot: Array[jl.annotation.Annotation] = owner.getParameterAnnotations()(index)
      findAnnotationOf[T](annot)
    }

    def get(obj: Any) = {
      sys.error("get for method parameter is not supported")
    }
  }

  case class Method(owner: Class[_], jMethod: jl.reflect.Method, name: String, params: Array[MethodParameter], returnType: Type) extends Type {
    override def toString = "Method(%s#%s, [%s], %s)".format(owner.getSimpleName, name, params.mkString(", "), returnType)

    //lazy val jMethod = owner.getMethod(name, params.map(_.rawType): _*)

    def findAnnotationOf[T <: jl.annotation.Annotation](implicit c: ClassManifest[T]): Option[T] = {
      jMethod.getAnnotation(c.erasure.asInstanceOf[Class[T]]) match {
        case null => None
        case a => Some(a.asInstanceOf[T])
      }
    }
    def findAnnotationOf[T <: jl.annotation.Annotation](paramIndex: Int)(implicit c: ClassManifest[T]): Option[T] = {
      params(paramIndex).findAnnotationOf[T]
    }

    override def hashCode = {
      owner.hashCode() + name.hashCode()
    }
  }

  case class Constructor(cl: Class[_], params: Array[ConstructorParameter]) extends Type {
    val name = cl.getSimpleName
    override def toString = "Constructor(%s, [%s])".format(cl.getSimpleName, params.mkString(", "))

    def newInstance(args: Array[AnyRef]): Any = {
      val cc = cl.getConstructors()(0)
      if (args.isEmpty)
        cc.newInstance()
      else
        cc.newInstance(args: _*)
    }
  }

  implicit def toSchema(cl: Class[_]): ObjectSchema = ObjectSchema(cl)

  private val schemaTable = new WeakHashMap[Class[_], ObjectSchema]

  /**
   * Get the object schema of the specified type. This method caches previously created ObjectSchema instances, and
   * second call for the same type object return the cached entry.
   */
  def apply(cl: Class[_]): ObjectSchema = schemaTable.getOrElseUpdate(cl, new ObjectSchema(cl))

  def of[A](implicit m: ClassManifest[A]): ObjectSchema = apply(m.erasure)

  def getSchemaOf(obj: Any): ObjectSchema = apply(obj.getClass)

  private val sigCache = new WeakHashMap[Class[_], Option[ScalaSig]]

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


    sigCache.getOrElseUpdate(cl, {
      val sig =
        try {
          trace("Searching for signature of %s", cl.getName)
          val s = ScalaSigParser.parse(cl)
          if (s.isDefined) {
            trace("Found signature of %s", cl.getSimpleName)
          }
          s
        }
        catch {
          // ScalaSigParser throws NPE when noe signature for the class is found
          case _ => None
        }
      sig.orElse(enclosingObject(cl).flatMap(findSignature(_)))
    })
  }

  def findFieldOwner(name: String, baseClass: Class[_]): Option[Class[_]] = {
    def isFieldOwner(cl: Class[_]) = {
      try {
        cl.getDeclaredField(name)
        true
      }
      catch {
        case _ => false
      }
    }

    if (isFieldOwner(baseClass))
      Some(baseClass)
    else {
      val parents = findParentClasses(baseClass)
      parents.foldLeft[Option[Class[_]]](None) {
        (opt, cl) => opt.orElse(findFieldOwner(name, cl))
      }
    }
  }

  private def findConstructor(cl: Class[_], sig: ScalaSig): Option[Constructor] = {

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



      val l = for (((name, vt), index) <- toAttribute(paramSymbols, sig, cl).zipWithIndex)
      yield {
        // resolve the actual field owner
        val fieldOwner = findFieldOwner(name, cl)

        if (fieldOwner.isEmpty)
          throw new IllegalStateException("No field owner is found: name:%s, base class:%s".format(name, cl.getSimpleName))
        ConstructorParameter(cl, fieldOwner.get, index, name, vt)
      }
      l.toArray
    }

    val entries = (0 until sig.table.length).map(sig.parseEntry(_))
    entries.collectFirst {
      case m: MethodType if isTargetClass(m.resultType) =>
        Constructor(cl, findConstructorParameters(m, sig))
    }
  }

  private def findConstructor(cl: Class[_]): Option[Constructor] = {
    findSignature(cl).flatMap(sig => findConstructor(cl, sig))
  }

  private def isSystemClass(cl: Class[_]) = {
    if (cl == null)
      true
    else {
      val name = cl.getName
      (name.startsWith("java.") || name == "scala" || name.startsWith("scala."))
    }
  }

  def getParentsByReflection(cl: Class[_]) = {
    trace("get parents of %s by reflecton", cl.getSimpleName)
    val i = if (cl.getInterfaces != null) cl.getInterfaces else Array.empty[Class[_]]
    val p = Seq(cl.getSuperclass) ++ i
    val filtered = p.filterNot(c => isSystemClass(c))
    trace("parents of %s: %s ", cl.getSimpleName, filtered.map(_.getName).mkString(", "))
    filtered
  }

  def findParentClasses(cl: Class[_]): Seq[Class[_]] = {
    getParentsByReflection(cl)

    //    findSignature(cl) match {
    //      case Some(sig) if sig.table.length > 0 =>
    //        val classInfo = parseEntries(sig).collectFirst {
    //          case c@ClassInfoType(symbol, typeRefs) if symbol.name == cl.getSimpleName => c
    //        }
    //        def loadClass(path: String): Option[Class[_]] = {
    //          val loader = Thread.currentThread().getContextClassLoader
    //          try
    //            Some(loader.loadClass(path))
    //          catch {
    //            case _ => None
    //          }
    //        }
    //        if (classInfo.isDefined) {
    //          val parents: Seq[String] = classInfo.get.typeRefs.collect {
    //            case t@TypeRefType(ThisType(prefix), symbol, typeArgs) if !isSystemPrefix(prefix.path) => symbol.path
    //          }
    //          parents.flatMap(loadClass(_))
    //        }
    //        else {
    //          // Use java reflection
    //          getParentsByReflection(cl)
    //        }
    //      case _ => getParentsByReflection(cl)
    //    }
  }

  def findParentSchemas(cl: Class[_]): Seq[ObjectSchema] = {
    findParentClasses(cl).map(ObjectSchema(_))
  }

  private def toAttribute(param: Seq[MethodSymbol], sig: ScalaSig, refCl: Class[_]): Seq[(String, ValueType)] = {
    val paramRefs = param.map(p => (p.name, sig.parseEntry(p.symbolInfo.info)))
    val paramSigs = paramRefs.map {
      case (name: String, t: TypeRefType) => (name, t)
    }

    for ((name, typeSignature) <- paramSigs) yield (name, resolveClass(typeSignature))
  }

  def isOwnedByTargetClass(m: MethodSymbol, cl: Class[_]): Boolean = {
    m.symbolInfo.owner match {
      case ClassSymbol(symbolInfo, _) => symbolInfo.name == cl.getSimpleName
      case _ => false
    }
  }

  private def parseEntries(sig: ScalaSig) = (0 until sig.table.length).map(sig.parseEntry(_))

  def parametersOf(cl: Class[_]): Array[Parameter] = {
    findSignature(cl) match {
      case None => Array.empty
      case Some(sig) => {
        val entries = parseEntries(sig)

        val parents = findParentSchemas(cl)
        val parentParams = parents.flatMap {
          p => p.parameters
        }.collect {
          //case c @ ConstructorParameter(owner, fieldOwner, index, name, valueType) => c
          //case m @ MethodParameter(owner, index, name, valueType) => m
          // Fix actual owner
          case FieldParameter(owner, ref, name, valueType) => {
            val fieldOwner = findFieldOwner(name, cl).getOrElse(cl)
            FieldParameter(fieldOwner, cl, name, valueType)
          }
        }

        val constructorParams = findConstructor(cl, sig) match {
          case None => Seq.empty[ConstructorParameter]
          case Some(cc) => cc.params.toSeq
        }

        def isFieldReader(m: MethodSymbol): Boolean = {
          m.isAccessor && !m.isParamAccessor && !m.isLazy && isOwnedByTargetClass(m, cl) && !m.name.endsWith("_$eq") && m.symbolInfo.privateWithin.isEmpty
        }

        val fieldParams = entries.collect {
          case m: MethodSymbol if isFieldReader(m) => {
            entries(m.symbolInfo.info) match {
              case NullaryMethodType(resultType: TypeRefType) => {
                FieldParameter(cl, cl, m.name, resolveClass(resultType))
              }
            }
          }
        }

        // Aggregate parameters
        val foundParams = (constructorParams.map(_.name) ++ fieldParams.map(_.name)).toSet
        val parentOnlyParams = parentParams.filterNot(p => foundParams.contains(p.name))

        (constructorParams ++ fieldParams ++ parentOnlyParams).toArray
      }
    }
  }

  def methodsOf(cl: Class[_]): Array[Method] = {
    val methods = findSignature(cl) match {
      case None => Array.empty[Method]
      case Some(sig) => {
        val entries = (0 until sig.table.length).map(sig.parseEntry(_))

        def isTargetMethod(m: MethodSymbol): Boolean = {
          // synthetic is used for functions returning default values of method arguments (e.g., ping$default$1)
          m.isMethod && !m.isPrivate && !m.isProtected && !m.isImplicit && !m.isSynthetic && !m.isAccessor && m.name != "<init>" && m.name != "$init$" && isOwnedByTargetClass(m, cl)
        }

        def resolveMethodArgTypes(params: Seq[(String, ValueType)]) = {
          params.map {
            case (name, vt) if TypeUtil.isArray(vt.rawType) => {
              val gt = vt.asInstanceOf[GenericType]
              val t = gt.genericTypes(0)
              val loader = Thread.currentThread.getContextClassLoader
              Class.forName("[L%s;".format(t.rawType.getName), false, loader)
            }
            case (name, vt) => vt.rawType
          }
        }

        val targetMethodSymbol : Seq[(MethodSymbol, Any)] = entries.collect {
          case m: MethodSymbol if isTargetMethod(m) => (m, entries(m.symbolInfo.info))
        }

        val methods = targetMethodSymbol.map { s =>
          try {
            s match {
              case (m: MethodSymbol, NullaryMethodType(resultType: TypeRefType)) => {
                val jMethod = cl.getMethod(m.name)
                Some(Method(cl, jMethod, m.name, Array.empty[MethodParameter], resolveClass(resultType)))
              }
              case (m: MethodSymbol, MethodType(resultType: TypeRefType, paramSymbols: Seq[_])) => {
                val params = toAttribute(paramSymbols.asInstanceOf[Seq[MethodSymbol]], sig, cl)
                val jMethod = cl.getMethod(m.name, resolveMethodArgTypes(params): _*)
                val mp = for (((name, vt), index) <- params.zipWithIndex) yield MethodParameter(jMethod, index, name, vt)
                Some(Method(cl, jMethod, m.name, mp.toArray, resolveClass(resultType)))
              }
              case _ => None
            }
          }
          catch {
            case e => {
              warn("error occurred when accessing method %s : %s", s, e)
              None
            }
          }
        }
        methods.collect{case Some(x) => x}.toArray
      }
    }

    val p = parentMethodsOf(cl)
    trace("parent methods of %s: %s", cl.getSimpleName, p.mkString(", "))
    (methods ++ p).toArray
  }

  def parentMethodsOf(cl: Class[_]) = {
    def resolveImplOwner(m: Method) {
      m.owner
    }
    findParentSchemas(cl).flatMap(_.methods).map {
      m =>
        m
    }
  }

  def resolveClass(typeSignature: TypeRefType): ValueType = {

    val name = typeSignature.symbol.path
    val clazz: Class[_] = {
      name match {
        // Resolve classes of primitive types.
        // This special treatment is necessary because classes of primitive types, classOf[scala.Int] etc. are converted by
        // Scala compiler into Java primitive types (e.g., int, float). So classOf[Int] is represented as classOf[int] internally.
        case "scala.Boolean" => classOf[Boolean]
        case "scala.Byte" => classOf[Byte]
        case "scala.Short" => classOf[Short]
        case "scala.Char" => classOf[Char]
        case "scala.Int" => classOf[Int]
        case "scala.Float" => classOf[Float]
        case "scala.Long" => classOf[Long]
        case "scala.Double" => classOf[Double]
        case "scala.Predef.String" => classOf[String]
        // Map and Set type names are defined in Scala.Predef
        case "scala.Predef.Map" => classOf[Map[_, _]]
        case "scala.Predef.Set" => classOf[Set[_]]
        case "scala.package.Seq" => classOf[Seq[_]]
        case "scala.package.List" => classOf[List[_]]
        case "scala.Any" => classOf[Any]
        case "scala.AnyRef" => classOf[AnyRef]
        case _ =>
          // Find the class using the context class loader
          val loader = Thread.currentThread().getContextClassLoader
          try loader.loadClass(name)
          catch {
            case _ => {
              // When class is defined inside an object, its class name has suffix '$' like "xerial.silk.SomeTest$A"
              val parent = typeSignature.symbol.parent
              val anotherClassName = "%s$%s".format(if (parent.isDefined) parent.get.path else "", typeSignature.symbol.name)
              loader.loadClass(anotherClassName)
            }
          }
      }
    }


    if (typeSignature.typeArgs.isEmpty) {
      StandardType(clazz)
    }
    else {
      val typeArgs: Seq[ValueType] = typeSignature.typeArgs.map {
        case x: TypeRefType => resolveClass(x)
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

  if (cl == null)
    throw new NullPointerException("input class is null")

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
  def findParameter(name: String): Option[Parameter] = {
    parameterIndex.get(name)
  }
  def containsParameter(name: String) = parameterIndex.contains(name)

  lazy val constructor: Constructor = {
    findConstructor(cl) match {
      case Some(c) => c
      case None => throw new IllegalArgumentException("no constructor is found for " + cl)
    }
  }

  override def toString = {
    if (parameters.isEmpty)
      name
    else
      "%s(%s)".format(name, parameters.mkString(", "))
  }
}


