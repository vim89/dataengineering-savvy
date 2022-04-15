package com.vitthalmirji.dataengineering.common

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import java.util.Optional
import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaSetConverter, mapAsScalaMapConverter}

/**
 * Singleton Object [[com.vitthalmirji.dataengineering.common.Converters]] has
 * implicit classes that can be used as library functions
 * to chain the function call without calling as function invoke on
 * previous returned value (along with type)
 */

object Converters {

  /**
   * [[com.vitthalmirji.dataengineering.common.Converters.JavaToScalaOption]] is an implicit
   * class that has function [[com.vitthalmirji.dataengineering.common.Converters.JavaToScalaOption#toOption()]]
   *
   * @param javaOptional is Java's Optional Type
   * @tparam T returns Scala's Option Type of generic type T
   */
  implicit class JavaToScalaOption[T](javaOptional: Optional[T]) extends Serializable {

    /**
     * [[com.vitthalmirji.dataengineering.common.Converters.JavaToScalaOption#toOption()]]
     * converts Java Optional[T] to Scala Option[T] generic type given at runtime
     *
     * @return returns Scala's Option Type of generic type T
     */
    def toOption: Option[T] = if (javaOptional.isPresent) Some(javaOptional.get()) else None
  }

  /**
   * [[com.vitthalmirji.dataengineering.common.Converters.JavaToScalaList]] is an implicit
   * class that has function [[com.vitthalmirji.dataengineering.common.Converters.JavaToScalaList#toScala()]]
   *
   * @param javaArray Java's java.util.List type
   * @tparam E returns Scala's List Type of generic type E
   */
  implicit class JavaToScalaList[E](javaArray: java.util.List[E]) extends Serializable {

    /**
     * [[com.vitthalmirji.dataengineering.common.Converters.JavaToScalaList#toScala()]]
     *
     * @return returns java.util.List[E] to Scala List[E]
     */
    def toScala: List[E] = javaArray.asScala.toList
  }

  /**
   * [[com.vitthalmirji.dataengineering.common.Converters.JavaToScalaSet]] is an implicit
   * class that has function [[com.vitthalmirji.dataengineering.common.Converters.JavaToScalaSet#toScala()]]
   *
   * @param javaSet Java's java.util.Set type
   * @tparam E returns Scala's Set Type of generic type E
   */

  implicit class JavaToScalaSet[E](javaSet: java.util.Set[E]) extends Serializable {

    /**
     * [[com.vitthalmirji.dataengineering.common.Converters.JavaToScalaSet#toScala()]]
     *
     * @return returns java's java.util.Set[E] type converted to Scala scala.Predef$#Set()
     */
    def toScala: Set[E] = javaSet.asScala.toSet
  }

  /**
   * [[com.vitthalmirji.dataengineering.common.Converters.JavaToScalaMap]] is an implicit
   * class that has function [[com.vitthalmirji.dataengineering.common.Converters.JavaToScalaMap#toScala()]]
   *
   * @param javaMap Java's java.util.Map type
   * @tparam K Generic Key of the map generic type
   * @tparam V Generic Value of the map generic
   * @return returns Scala's Map of K, V
   */
  implicit class JavaToScalaMap[K, V](javaMap: java.util.Map[K, V]) extends Serializable {

    /**
     * [[com.vitthalmirji.dataengineering.common.Converters.JavaToScalaMap#toScala()]]
     *
     * @return returns java's Map type converted to Scala Map
     */
    def toScala: Map[K, V] = javaMap.asScala.toMap
  }

  /**
   * [[com.vitthalmirji.dataengineering.common.Converters.VariousTypesFromJsonString]]
   * is an implicit class that provides conversion functions viz.
   * [[com.vitthalmirji.dataengineering.common.Converters.VariousTypesFromJsonString#toMap(boolean, scala.reflect.Manifest)]],
   * [[com.vitthalmirji.dataengineering.common.Converters.VariousTypesFromJsonString#jsonStringAs(boolean, scala.reflect.Manifest)]]
   *
   * @param jsonString Json as String
   */
  implicit class VariousTypesFromJsonString(jsonString: Option[String]) extends Serializable {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    /**
     * [[com.vitthalmirji.dataengineering.common.Converters.VariousTypesFromJsonString#toMap(boolean, scala.reflect.Manifest)]]
     * function convertes json from string format to Map of [String, generic] given at runtime
     *
     * @param failOnUnknownJsonProperties by default set to false, setting this property to 'true' will fail
     *                                    conversion of jsonString to type T if properties are undefined / unknown
     * @param m                           manifest of generic Type T given for conversion
     * @tparam T Generic type into which conversion is required
     * @return returns Option[ Map[ String, T ] ] for safe null handling
     */
    def toMap[T](failOnUnknownJsonProperties: Boolean = false)(implicit m: Manifest[T]): Option[Map[String, T]] =
      jsonStringAs[Map[String, T]](failOnUnknownJsonProperties)

    /**
     * [[com.vitthalmirji.dataengineering.common.Converters.VariousTypesFromJsonString#jsonStringAs(boolean, scala.reflect.Manifest)]]
     * function convertes json from string format to generic type given at runtime
     *
     * @param failOnUnknownJsonProperties by default set to false, setting this property to 'true' will fail
     *                                    conversion of jsonString to type T if properties are undefined / unknown
     * @param m                           manifest of generic Type T given for conversion
     * @tparam T Generic type into which conversion is required
     * @return returns Option [T] for safe null handling
     *         Usage: {{{
     * val jsonString: String =
     * """{"name":"Walmart","plotNo":10,"blocks":["A","B"],"address":"Whitefield",
     * |"city":"Bangalore","employees":[{"id":1,"name":"Emp1","age":25,"isVendor":false,
     * |"projects":["P1","P2"]},
     * |{"id":2,"name":"Emp2","age":21,"isVendor":true,"projects":["P1"]}]}""".stripMargin
     * case class Employee(id: Int, name: String, age: Int, isVendor: Boolean, projects: Array[String])
     * case class Office(name: String, plotNo: Int, blocks: Array[String], address: String, city: String,
     * employees: Array[Employee])
     *
     * val office: Office = jsonString.jsonStringAs[Office]()
     *
     * Using with CCM Config -
     * val ccmConfig: CcmConfig = new CcmConfig(stratiServiceProvider = StratiServiceProvider.getInstance()){}
     * val config = ccmConfig.getCcmConfig("appConfig")
     *
     * val office = config.flatMap(c => c.get("office").jsonStringAs[Office]())
     * assert(office.isDefined)
     * assert(office.exists(_.name.equals("Walmart")))
     * assert(office.exists(_.plotNo.equals(10)))
     * assert(office.exists(_.employees.length.equals(2)))
     * }}}
     */
    def jsonStringAs[T](failOnUnknownJsonProperties: Boolean = false)(implicit m: Manifest[T]): Option[T] = {
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, failOnUnknownJsonProperties)
      jsonString.map(s => mapper.readValue[T](s))
    }
  }
}
