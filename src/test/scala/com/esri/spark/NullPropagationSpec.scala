package com.esri.spark

import java.io.File

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, Literal, UnsafeProjection}
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Every GeoFunction expression returns null when any argument is null, in interpreted and
 * generated evaluation alike. Expressions are discovered from the compiled classes, so a new
 * one is covered without being listed here.
 * Run with: mvn -DskipTests=false -Dsuites=com.esri.spark.NullPropagationSpec test
 */
class NullPropagationSpec extends AnyFlatSpec with Matchers {

  // ST_ISEMPTY treats a null geometry as empty.
  private val NullIsTrue = Set("STIsEmpty")

  private val expressions: Seq[Class[_]] = {
    val dir = new File(new File(classOf[STPoint].getProtectionDomain.getCodeSource.getLocation.toURI), "com/esri/spark")
    dir.listFiles().map(_.getName).filter(n => n.endsWith(".class") && !n.contains("$"))
      .map(n => Class.forName("com.esri.spark." + n.stripSuffix(".class")))
      .filter(c => classOf[Expression].isAssignableFrom(c))
      .sortBy(_.getSimpleName)
      .toSeq
  }

  private def build(c: Class[_], children: Seq[Expression]): Expression = {
    c.getConstructors.find(_.getParameterCount == 1) match {
      case Some(k) if k.getParameterTypes.head == classOf[Seq[_]] =>
        k.newInstance(children).asInstanceOf[Expression]
      case _ =>
        c.getConstructors.find(_.getParameterCount == children.length).get
          .newInstance(children: _*).asInstanceOf[Expression]
    }
  }

  private def inputTypes(c: Class[_]): Seq[DataType] = {
    val arity = c.getConstructors.map(_.getParameterCount).max
    build(c, Seq.fill(if (arity == 1) 5 else arity)(Literal(0.0))) match {
      case e: ImplicitCastInputTypes => e.inputTypes.map {
        case StructType => StructType(Nil) // GDB shape: any struct is accepted
        case t: DataType => t
      }
    }
  }

  "GeoFunction expressions" should "be discovered" in {
    expressions.map(_.getSimpleName) should contain allOf("STPoint", "QRList", "ClipLineDist", "STIsEmpty")
    expressions.size should be > 60
  }

  expressions.foreach { c =>
    it should s"propagate every null argument of ${c.getSimpleName}" in {
      val types = inputTypes(c)
      types.indices.foreach { nullIndex =>
        val args = types.zipWithIndex.map { case (t, i) =>
          if (i == nullIndex) Literal.create(null, t) else Literal.default(t)
        }
        val e = build(c, args)
        withClue(s"${c.getSimpleName} argument $nullIndex: ") {
          val generated = UnsafeProjection.create(Seq(e))(InternalRow.empty)
          if (NullIsTrue(c.getSimpleName)) {
            e.eval(InternalRow.empty) shouldBe true
            generated.getBoolean(0) shouldBe true
          } else {
            e.nullable shouldBe true
            Option(e.eval(InternalRow.empty)) shouldBe None
            generated.isNullAt(0) shouldBe true
          }
        }
      }
    }
  }
}
