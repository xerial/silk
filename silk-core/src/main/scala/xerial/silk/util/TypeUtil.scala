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

package xerial.silk.util

import java.{lang => jl}
import java.io.File
import java.util.Date
import java.text.DateFormat
import java.lang.Byte
import xerial.silk.lens.ObjectSchema
import java.lang.{reflect => jr}
import collection.mutable.{ArrayBuffer, Builder}
import collection.mutable
import xerial.silk.lens.ObjectSchema.{ValueType, GenericType}

//--------------------------------------
//
// TypeUtil.scala
// Since: 2012/01/11 12:46
//
//--------------------------------------

/**
 * Utility for manipulating objects using reflection
 * @author leo
 */
object TypeUtil extends Logger {

  implicit def toClassManifest[T](targetType: Class[T]): ClassManifest[T] = ClassManifest.fromClass(targetType)

  // primitive type names
  object BasicType extends Enumeration {
    val Boolean, Int, String, Float, Double, Long, Short, Byte, Char, File, Date, Enum, Other = Value
  }

  val javaPrimitiveObjectTypes =
    Set[Class[_]](classOf[jl.Integer], classOf[jl.Short], classOf[jl.Long],
      classOf[jl.Float], classOf[jl.Byte], classOf[jl.Double], classOf[jl.Boolean], classOf[jl.String]
      //  , jl.Integer.TYPE, jl.Short.TYPE, jl.Long.TYPE, jl.Float.TYPE, jl.Byte.TYPE, jl.Boolean.TYPE
    )
  val scalaPrimitiveTypes: Set[Class[_]] =
    Set[Class[_]](classOf[Int], classOf[Short], classOf[Long], classOf[Float], classOf[Byte],
      classOf[Double], classOf[Boolean])

  def isPrimitive(cl: Class[_]): Boolean = {
    cl.isPrimitive || javaPrimitiveObjectTypes.contains(cl) || scalaPrimitiveTypes.contains(cl)
  }

  def isEnumeration[T](cl: ClassManifest[T]): Boolean = {
    cl <:< classManifest[Enumeration$Value]
  }

  def isOption[T](cl: ClassManifest[T]): Boolean = {
    cl <:< classOf[Option[_]]
  }

  def isArray[T](cl: Class[T]) = {
    cl.isArray || cl.getSimpleName == "Array"
  }

  def elementType[T](cl: Class[T]) = {
    cl.getComponentType
  }

  def canBuildFromBuffer[T](cl: ClassManifest[T]) = isArray(cl.erasure) || isSeq(cl) || isMap(cl) || isSet(cl)

  def isTraversableOnce[T](cl: ClassManifest[T]) = cl <:< classOf[TraversableOnce[_]]

  def isBuffer[T](cl: ClassManifest[T]) = {
    cl <:< classOf[mutable.Buffer[_]]
  }

  def isSeq[T](cl: ClassManifest[T]) = {
    cl <:< classOf[Seq[_]]
  }

  def isMap[T](cl: ClassManifest[T]) = {
    cl <:< classOf[Map[_, _]]
  }

  def isSet[T](cl: ClassManifest[T]) = {
    cl <:< classOf[Set[_]]
  }

  def isTuple[T](cl: ClassManifest[T]) = {
    cl <:< classOf[Product]
  }

  private val basicTypeTable = Cache[Class[_], BasicType.Value] {
    toBasicType
  }
  /**
   * Helper method to translate primitive types into BasicType enumerations
   */
  implicit def basicTypeOf(cl: Class[_]): BasicType.Value = basicTypeTable(cl)

  private[util] def toBasicType[_](cl: Class[_]): BasicType.Value = {
    toClassManifest(cl) match {
      case c if c == classManifest[String] => BasicType.String
      case c if (c == ClassManifest.Boolean || c == classManifest[java.lang.Boolean]) => BasicType.Boolean
      case c if (c == ClassManifest.Int || c == classManifest[java.lang.Integer]) => BasicType.Int
      case c if (c == ClassManifest.Float || c == classManifest[java.lang.Float]) => BasicType.Float
      case c if (c == ClassManifest.Double || c == classManifest[java.lang.Double]) => BasicType.Double
      case c if (c == ClassManifest.Long || c == classManifest[java.lang.Long]) => BasicType.Long
      case c if (c == ClassManifest.Short || c == classManifest[java.lang.Short]) => BasicType.Short
      case c if (c == ClassManifest.Byte || c == classManifest[java.lang.Byte]) => BasicType.Byte
      case c if (c == ClassManifest.Char || c == classManifest[java.lang.Character]) => BasicType.Char
      case c if c == classManifest[File] => BasicType.File
      case c if c == classManifest[Date] => BasicType.Date
      case c if c <:< classManifest[Enumeration$Value] => BasicType.Enum
      case _ => BasicType.Other
    }
  }

  def toBuffer[A](input: Array[A]): collection.mutable.Buffer[A] = {
    input.toBuffer[A]
  }

  /**
   * Convert immutable collections or arrays to a mutable buffer  
   * @param input
   * @param valueType
   */
  def toBuffer(input: Any, valueType: ObjectSchema.ValueType): collection.mutable.Buffer[_] = {

    def err = throw new IllegalArgumentException("cannot convert to ArrayBuffer: %s".format(valueType))

    if (!canBuildFromBuffer(valueType.rawType))
      err

    val cl: Class[_] = input.getClass
    if (isArray(cl)) {
      val a = input.asInstanceOf[Array[_]]
      a.toBuffer
    }
    else if (isTraversableOnce(cl) && valueType.isGenericType) {
      val gt = valueType.asInstanceOf[ObjectSchema.GenericType]
      val e = gt.genericTypes(0).rawType
      type E = e.type
      val l = input.asInstanceOf[TraversableOnce[E]]
      val b = new ArrayBuffer[E]
      l.foreach(b += _)
      b
    }
    else
      err
  }

  def convert(value: Any, targetType: ObjectSchema.ValueType): Any = {
    if (targetType.isOption) {
      Some(convert(value, targetType.rawType))
    }
    else {
      val t: Class[_] = targetType.rawType
      val s: Class[_] = value.getClass
      if (t.isAssignableFrom(s))
        value
      else if (isBuffer(s)) {
        val buf = value.asInstanceOf[mutable.Buffer[_]]
        val gt: Seq[ValueType] = targetType.asInstanceOf[GenericType].genericTypes
        val e = gt(0).rawType
        type E = e.type
        if (isArray(t)) {
          val arr = e.newArray(buf.length).asInstanceOf[Array[Any]]
          buf.copyToArray(arr)
          arr
        }
        else if (isSeq(t)) {
          buf.toSeq
        }
        else if (isSet(t)) {
          buf.toSet
        }
        else if (isMap(t)) {
          buf.asInstanceOf[mutable.Buffer[(_, _)]].toMap
        }
        else
          throw sys.error("cannot convert %s to %s".format(s.getSimpleName, t.getSimpleName))
      }
      else
        convert(value, targetType.rawType)
    }
  }

  /**
   * Convert the input value into the target type
   */
  def convert[A](value: Any, targetType: Class[A]): A = {
    val cl: Class[_] = value.getClass
    if (targetType.isAssignableFrom(cl))
      value.asInstanceOf[A]
    else {
      stringConstructor(targetType) match {
        case Some(cc) => cc.newInstance(value.toString).asInstanceOf[A]
        case None => convertToBasicType(value, targetType)
      }
    }
  }

  /**
   * Convert the input value into the target type
   */
  def convertToBasicType[A](value: Any, targetType: BasicType.Value): A = {
    val s = value.toString
    val v: Any = targetType match {
      case BasicType.String => s
      case BasicType.Boolean => s.toBoolean
      case BasicType.Int => s.toInt
      case BasicType.Float => s.toFloat
      case BasicType.Double => s.toDouble
      case BasicType.Long => s.toLong
      case BasicType.Short => s.toShort
      case BasicType.Byte => s.toByte
      case BasicType.Char if (s.length == 1) => s(0)
      case BasicType.File => new File(s)
      case BasicType.Date => DateFormat.getDateInstance.parse(s)
      case BasicType.Enum => throw new IllegalArgumentException("""Scala Enumeration (%s) cannot be set with convert(). Use setField instead: value:%s""" format(targetType.toString, s))
      case _ =>
        throw new IllegalArgumentException("""Failed to convert "%s" to %s""".format(s, targetType.toString))
    }
    v.asInstanceOf[A]
  }

  def stringConstructor(cl: Class[_]): Option[jr.Constructor[_]] = {
    val cc = cl.getDeclaredConstructors
    cc.find {
      cc =>
        val pt = cc.getParameterTypes
        pt.length == 1 && pt(0) == classOf[String]
    }
  }

  def zero[A](cl: Class[A]): A = {
    if (isPrimitive(cl)) {
      val v: Any = basicTypeOf(cl) match {
        case BasicType.String => ""
        case BasicType.Boolean => true
        case BasicType.Int => 0
        case BasicType.Float => 0f
        case BasicType.Double => 0.0
        case BasicType.Long => 0L
        case BasicType.Short => 0.toShort
        case BasicType.Byte => 0.toByte
        case BasicType.Char => 0.toChar
        case _ => {
          if (hasDefaultConstructor(cl))
            cl.newInstance
        }
      }
      v.asInstanceOf[A]
    }
    else if (isArray(cl)) {
      elementType(cl).newArray(0).asInstanceOf[A]
    }
    else if (isMap(cl)) {
      Map.empty.asInstanceOf[A]
    }
    else if (isSeq(cl)) {
      Seq.empty.asInstanceOf[A]
    }
    else if (isSet(cl)) {
      Set.empty.asInstanceOf[A]
    }
    else if (isOption(cl)) {
      None.asInstanceOf[A]
    }
    else if (isTuple(cl)) {
      val c = cl.getDeclaredConstructors()(0)
      val elementType = cl.getTypeParameters
      val arity = elementType.length
      val args = for (i <- 1 to arity) yield {
        val m = cl.getMethod("_%d".format(i))
        zero(m.getReturnType).asInstanceOf[AnyRef]
      }
      newInstance(cl, args.toSeq)
    }
    else if (canInstantiate(cl)) {
      newInstance(cl).asInstanceOf[A]
    }
    else
      null.asInstanceOf[A]
  }

  def hasDefaultConstructor[A](cl: Class[A]) = {
    cl.getConstructors.find(x => x.getParameterTypes.length == 0).isDefined
  }

  def canInstantiate[A](cl: Class[A]): Boolean = {
    if (isPrimitive(cl) || hasDefaultConstructor(cl))
      return true

    val fields = cl.getDeclaredFields
    val c = cl.getConstructors().find {
      x =>
        val p = x.getParameterTypes
        if (p.length != fields.length)
          return false

        fields.zip(p).forall(e =>
          e._1.getType == e._2)
    }

    c.isDefined
  }

  /**
   * update an element of the array. This method is useful when only the element type information of the array is available
   */
  def updateArray(array: Any, elementType: Class[_], i: Int, v: Any) {
    val bt = basicTypeOf(elementType)
    bt match {
      case BasicType.String => array.asInstanceOf[Array[String]].update(i, convertToBasicType[String](v, bt))
      case BasicType.Boolean => array.asInstanceOf[Array[Boolean]].update(i, convertToBasicType[Boolean](v, bt))
      case BasicType.Int => array.asInstanceOf[Array[Int]].update(i, convertToBasicType[Int](v, bt))
      case BasicType.Float => array.asInstanceOf[Array[Float]].update(i, convertToBasicType[Float](v, bt))
      case BasicType.Double => array.asInstanceOf[Array[Double]].update(i, convertToBasicType[Double](v, bt))
      case BasicType.Long => array.asInstanceOf[Array[Long]].update(i, convertToBasicType[Long](v, bt))
      case BasicType.Short => array.asInstanceOf[Array[Short]].update(i, convertToBasicType[Short](v, bt))
      case BasicType.Byte => array.asInstanceOf[Array[Byte]].update(i, convertToBasicType[Byte](v, bt))
      case BasicType.Char => array.asInstanceOf[Array[Char]].update(i, convertToBasicType[Char](v, bt))
      case BasicType.File => array.asInstanceOf[Array[File]].update(i, convertToBasicType[File](v, bt))
      case BasicType.Date => array.asInstanceOf[Array[Date]].update(i, convertToBasicType[Date](v, bt))
      case _ => {
      }
      throw new IllegalArgumentException("failed to update array")
    }
  }

  def createEnumValue(prevEnum: Any, newEnum: Any, valueType: Class[_]): Option[_] = {
    val cl = ClassManifest.fromClass(valueType)
    val outer = cl.erasure.getDeclaredField("scala$Enumeration$$outerEnum")
    access(outer) {
      val enclosingEnumType: Enumeration = outer.get(prevEnum).asInstanceOf[Enumeration]
      val name = newEnum.toString.toLowerCase
      val v = enclosingEnumType.values.find(_.toString.toLowerCase == name)
      if (v.isEmpty)
        warn("unknown enum value %s".format(newEnum))
      v
    }
  }

  def updateEnumField(obj: Any, f: jr.Field, enumValue: Any): Unit = {
    val prevEnum = readField(obj, f).asInstanceOf[Enumeration$Value]
    val e = createEnumValue(prevEnum, enumValue, f.getType)
    e match {
      case Some(enum) => access(f) {
        f.set(obj, enum)
      }
      case None => // do nothing
    }
  }

  /**
   * Set the accessibility flag of fields and methods if they are not accessible, then
   * do some operation, and reset the accessibility properly upon the completion.
   */
  private[util] def access[A <: jr.AccessibleObject, B](f: A)(body: => B): B = {
    val accessible = f.isAccessible
    try {
      if (!accessible)
        f.setAccessible(true)
      body
    }
    finally {
      if (!accessible)
        f.setAccessible(false)
    }
  }

  def readField(obj: Any, f: jr.Field): Any = {
    access(f) {
      f.get(obj)
    }
  }

  def getTypeParameters(f: jr.Field): Array[Class[_]] = {
    getTypeParameters(f.getGenericType)
  }

  def getTypeParameters(gt: jr.Type): Array[Class[_]] = {
    gt match {
      case p: jr.ParameterizedType => {
        p.getActualTypeArguments.map(resolveClassType(_)).toArray
      }
    }
  }

  def resolveClassType(t: jr.Type): Class[_] = {
    t match {
      case p: jr.ParameterizedType => p.getRawType.asInstanceOf[Class[_]]
      case c: Class[_] => c
      case _ => classOf[Any]
    }
  }

  /**
   * Update the field value in the given object.
   *
   * @param obj
   * @param f
   * @param value
   */
  def setField(obj: Any, f: jr.Field, value: Any): Unit = {
    def getOrElse[T](default: => T) = {
      val e = f.get(obj)
      if (e == null) default else e.asInstanceOf[T]
    }

    def prepareInstance(prevValue: Option[_], newValue: Any, targetType: Class[_]): Option[_] = {
      if(isOption(targetType)) {
        val elementType = getTypeParameters(f)(0)
        Some(prepareInstance(prevValue, newValue, elementType))
      }
      else if (isEnumeration(targetType)) {
        createEnumValue(prevValue.get, newValue, targetType)
      }
      else
        Some(convert(newValue, targetType))
    }


    access(f) {
      val fieldType = f.getType
      val currentValue = f.get(obj)
      val newValue = prepareInstance(Some(currentValue), value, fieldType)
      if (newValue.isDefined)
        f.set(obj, newValue.get)
    }

  }

  def companionObject[A](cl: Class[A]): Option[Any] = {
    try {
      val companion = Class.forName(cl.getName + "$")
      val companionObj = companion.newInstance()
      Some(companionObj)
    }
    catch {
      case _ => None
    }
  }

  def defaultConstructorParameters[A](cl: Class[A]): Seq[AnyRef] = {
    val cc = cl.getConstructors()(0)
    val p = cc.getParameterTypes

    // Search for default parameter values
    //val hasOuter = cl.getDeclaredFields.find(x => x.getName == "$outer").isDefined
    //val numParamStart = if (hasOuter) 1 else 0
    val companion = companionObject(cl)
    val paramArgs = for (i <- 0 until p.length) yield {
      val defaultValue =
        if (companion.isDefined) {
          val methodName = "init$default$%d".format(i + 1)
          try {
            val m = companion.get.getClass.getDeclaredMethod(methodName)
            m.invoke(companion.get)
          }
          catch {
            // When no method for the initial value is found, use 'zero' value of the type
            case e => {
              zero(p(i))
            }
          }
        }
        else
          zero(p(i))
      defaultValue.asInstanceOf[AnyRef]
    }
    paramArgs
  }

  def newInstance[A, B <: AnyRef](cl: Class[A], args: Seq[B]): A = {
    val cc = cl.getConstructors()(0)
    val obj = cc.newInstance(args: _*)
    obj.asInstanceOf[A]
  }

  def newInstance[A](cl: Class[A]): A = {
    def createDefaultInstance: A = {
      val hasOuter = cl.getDeclaredFields.find(x => x.getName == "$outer").isDefined
      if (hasOuter)
        throw new IllegalArgumentException("Cannot use inner class %s. Use classes defined globally or in companion objects".format(cl.getName))
      val paramArgs = defaultConstructorParameters(cl)
      val cc = cl.getConstructors()(0)
      val obj = cc.newInstance(paramArgs: _*)
      obj.asInstanceOf[A]
    }

    try {
      val c = cl.getConstructor()
      cl.newInstance.asInstanceOf[A]
    }
    catch {
      case e: NoSuchMethodException => createDefaultInstance
    }
  }

}