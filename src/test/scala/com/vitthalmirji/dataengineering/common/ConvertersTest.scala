package com.vitthalmirji.dataengineering.common

import com.vitthalmirji.dataengineering.common.Converters.{JavaToScalaList, JavaToScalaMap, JavaToScalaOption, JavaToScalaSet}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import java.util
import java.util.Optional
import com.vitthalmirji.dataengineering.common.Converters.VariousTypesFromJsonString

class ConvertersTest extends AnyFunSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {}

  test("toOption") {
    val javaOptional: Optional[Int] = Optional.of[Int](4)
    val scalaOption: Option[Int]    = javaOptional.toOption
    assert(scalaOption.isDefined)
    assert(scalaOption.isInstanceOf[Option[Int]])
  }

  test("toScala for Java Set[E]") {
    val javaSet: java.util.Set[Int] = new util.HashSet[Int]()
    javaSet.add(100)
    javaSet.add(200)
    javaSet.add(100)

    val scalaSet: Set[Int] = javaSet.toScala
    assert(scalaSet.nonEmpty)
    assert(scalaSet.equals(Set(100, 200)))
  }

  test("toScala for Java List[E]") {
    val javaList: java.util.List[Int] = new util.ArrayList[Int]()
    javaList.add(100)
    javaList.add(200)
    javaList.add(300)

    val scalaList: List[Int] = javaList.toScala
    assert(scalaList.nonEmpty)
    assert(scalaList.equals(List(100, 200, 300)))
  }

  test("toScala for Java Map[K, V]") {
    val javaMap: java.util.Map[String, Int] = new util.HashMap[String, Int]()
    javaMap.put("One", 1)
    javaMap.put("Two", 2)
    javaMap.put("Three", 3)

    val scalaMap: Map[String, Int] = javaMap.toScala
    assert(scalaMap.nonEmpty)
    assert(scalaMap.equals(Map("One" -> 1, "Two" -> 2, "Three" -> 3)))
  }

  test("json String to Map[String, T]") {
    val jsonString: Option[String] =
      Some("""
        |{"a":[1,2],"b":[3,4,5],"c":[]}
        |""".stripMargin)
    val jsonAsMap: Option[Map[String, Seq[Int]]] = jsonString.toMap[Seq[Int]]()
    assert(jsonAsMap.isDefined)
    assert(jsonAsMap.map(_.contains("a")).isDefined)
    assert(jsonAsMap.map(_.get("c").map(_.isEmpty)).contains(Some(true)))
  }
}
