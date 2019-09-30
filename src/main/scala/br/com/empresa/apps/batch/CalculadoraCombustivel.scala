package br.com.empresa.apps.batch


import br.com.empresa.domain.{DadosCombustivel, DadosCombustivel2, DadosCombustivel3}
import br.com.empresa.engine.Engine
import br.com.empresa.input.DataInput
import br.com.empresa.spark.Spark
import org.apache.log4j.Level
import org.joda.time.DateTime
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object CalculadoraCombustivel {
  val ss: SparkSession = Spark.getSession
  val loadData = new DataInput
  val engine = new Engine(ss)

  def main(args: Array[String]): Unit = {
   import ss.implicits._

    Spark.setLogLevel(Level.ERROR)

    val x = loadData.loadCSV(ss)
    val formated: Dataset[DadosCombustivel3] = engine.formatToDadosCombustivel(x).as[DadosCombustivel3]
    val parsed = engine.parseToDadosCombustivel(formated)

    val groupedMonth = engine.calculateAverageByMonth(parsed,"municipio")
    val groupedState = engine.calculateAverage(parsed, "estado")
    val groupedRegion = engine.calculateAverage(parsed, "regiao")
    val groupedMont = engine.calculateAverageByMonth(parsed,"municipio")
    val data = engine.joinDS(parsed,groupedMont)
    val variance = engine.calculateVariance(data)
    val variation = engine.calculateVariation(data)
    val maxDifference = engine.calculateDifference(data)

    println("a) Estes valores estão distribuídos em dados semanais, agrupe eles por mês e calcule as médias de valores de cada combustível por cidade.")
    groupedMonth.sort("monthYearAverage", "municipioAverage").show(truncate = false, numRows = 4000)

    println("b) Calcule a média de valor do combustível por estado e região.")
    println("por estado")
    groupedState.sort("estado", "produto").show(truncate = false, numRows = 4000)
    println("por região")
    groupedRegion.sort("regiao", "produto").show(truncate = false, numRows = 4000)

    println("c) Calcule a variância e a variação absoluta do máximo, mínimo de cada cidade, mês a mês.")
    println("variância")
    variance.show(truncate = false, numRows = 4000)
    println("variação absoluta")
    variation.show(truncate = false, numRows = 4000)

    println("d) Quais são as 5 cidades que possuem a maior diferença entre o combustível mais barato e o mais caro.")
    maxDifference.map(p=> println(p))

  }

}
