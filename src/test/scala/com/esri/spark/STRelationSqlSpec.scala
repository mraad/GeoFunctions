package com.esri.spark

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * The predicates through the SQL analyzer: two arguments default wkid, and a bad arity is a
 * clean AnalysisException, not an analyzer AssertionError from zipping children with types.
 * Run with: mvn -DskipTests=false -Dsuites=com.esri.spark.STRelationSqlSpec test
 */
class STRelationSqlSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder().master("local[1]").appName("STRelationSqlSpec")
      .config("spark.ui.enabled", "false").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    Registry.registerFunctions()
  }

  override protected def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    } finally {
      super.afterAll()
    }
  }

  private val inside = "ST_FROMTEXT('POINT (1 1)')"
  private val box = "ST_FROMTEXT('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))')"

  "ST_INTERSECTS in SQL" should "accept two or three arguments" in {
    spark.sql(s"SELECT ST_INTERSECTS($inside, $box), ST_INTERSECTS($inside, $box, '-1')")
      .collect().head.toSeq shouldBe Seq(true, true)
  }

  it should "reject four arguments with an analysis error naming the arity" in {
    val e = the[AnalysisException] thrownBy
      spark.sql(s"SELECT ST_WITHIN($inside, $box, '-1', '-1')").collect()
    e.getMessage should include("requires 2 or 3 arguments")
  }
}
