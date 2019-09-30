package br.com.empresa.engine

import br.com.empresa.domain.{DadosCombustivel, DadosCombustivel2, DadosCombustivel3, DadosCombustivelAverage, DadosCombustivelDifference, DadosCombustivelVariance, DadosCombustivelVariation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ColumnName, DataFrame, Dataset, SparkSession}

class Engine (ss: SparkSession) {
  import ss.implicits._

  def formatToDadosCombustivel(df: DataFrame) = {
    df.withColumnRenamed("DATA INICIAL", "dataInicial")
      .withColumnRenamed("DATA FINAL", "dataFinal")
      .withColumnRenamed("REGIÃO", "regiao")
      .withColumnRenamed("ESTADO", "estado")
      .withColumnRenamed("MUNICÍPIO", "municipio")
      .withColumnRenamed("REGIÃO", "regiao")
      .withColumnRenamed("ESTADO", "estado")
      .withColumnRenamed("MUNICÍPIO", "municipio")
      .withColumnRenamed("PRODUTO", "produto")
      .withColumnRenamed("NÚMERO DE POSTOS PESQUISADOS", "numeroDePostosPesquisados")
      .withColumnRenamed("UNIDADE DE MEDIDA", "unidadeDeMedida")
      .withColumnRenamed("PREÇO MÉDIO REVENDA", "precoMedioRevenda")
      .withColumnRenamed("DESVIO PADRÃO REVENDA", "desvioPadraoRevenda")
      .withColumnRenamed("PREÇO MÍNIMO REVENDA", "precoMinimoRevenda")
      .withColumnRenamed("PREÇO MÁXIMO REVENDA", "precoMaximoRevenda")
      .withColumnRenamed("MARGEM MÉDIA REVENDA", "margemMediaRevenda")
      .withColumnRenamed("COEF DE VARIAÇÃO REVENDA", "coefDeVariaçãoRevenda")
      .withColumnRenamed("PREÇO MÉDIO DISTRIBUIÇÃO", "precoMedioDistribuicao")
      .withColumnRenamed("DESVIO PADRÃO DISTRIBUIÇÃO", "desvioPadraoDistribuicao")
      .withColumnRenamed("PREÇO MÉDIO REVENDA", "precoMedioRevenda")
      .withColumnRenamed("PREÇO MÍNIMO DISTRIBUIÇÃO", "precoMinimoDistribuicao")
      .withColumnRenamed("PREÇO MÁXIMO DISTRIBUIÇÃO", "precoMaximoDistribuicao")
      .withColumnRenamed("COEF DE VARIAÇÃO DISTRIBUIÇÃO", "coefDeVariaçãoDistribuicao")
  }

  def parseToDadosCombustivel(ds: Dataset[DadosCombustivel3]): Dataset[DadosCombustivel2] = {

    ds.map(p => DadosCombustivel.parseTo(p))
  }

  def calculateAverage(df: Dataset[DadosCombustivel2], columnName: String): DataFrame = {

    df.groupBy("produto", columnName)
      .agg(avg("precoMedioRevenda"),
        avg("precoMinimoRevenda"),
        avg("precoMaximoRevenda"),
        avg("margemMediaRevenda"),
        avg("precoMedioDistribuicao"),
        avg("precoMinimoDistribuicao"),
        avg("precoMaximoDistribuicao")
      )
  }

  def calculateAverageByMonthAndProduct(df: Dataset[DadosCombustivel2], columnName: String): DataFrame = {

    df.groupBy("monthYear", "produto", columnName)
      .agg(avg("precoMedioRevenda"),
        avg("precoMinimoRevenda"),
        avg("precoMaximoRevenda"),
        avg("margemMediaRevenda"),
        avg("precoMedioDistribuicao"),
        avg("precoMinimoDistribuicao"),
        avg("precoMaximoDistribuicao")
      )
  }

  def calculateAverageByMonth(df: Dataset[DadosCombustivel2], columnName: String): DataFrame = {

    df.groupBy("monthYear", columnName)
      .agg(avg("precoMedioRevenda"),
        avg("precoMinimoRevenda"),
        avg("precoMaximoRevenda"),
        avg("margemMediaRevenda"),
        avg("precoMedioDistribuicao"),
        avg("precoMinimoDistribuicao"),
        avg("precoMaximoDistribuicao")
      )
      .withColumnRenamed("monthYear", "monthYearAverage")
      .withColumnRenamed("municipio", "municipioAverage")
  }

  def joinDS(parsed: Dataset[DadosCombustivel2], df: DataFrame) = {
    parsed.join(df,
      parsed.col("monthYear") === df.col("monthYearAverage") &&
      parsed.col("municipio") === df.col("municipioAverage"))
      .drop("municipioAverage")
      .drop("monthYearAverage")
      .withColumnRenamed("avg(precoMedioRevenda)", "avgPrecoMedioRevenda")
      .withColumnRenamed("avg(precoMinimoRevenda)", "avgPrecoMinimoRevenda")
      .withColumnRenamed("avg(precoMaximoRevenda)", "avgPrecoMaximoRevenda")
      .withColumnRenamed("avg(margemMediaRevenda)", "avgMargemMediaRevenda")
      .withColumnRenamed("avg(precoMedioDistribuicao)", "avgPrecoMedioDistribuicao")
      .withColumnRenamed("avg(precoMinimoDistribuicao)", "avgPrecoMinimoDistribuicao")
      .withColumnRenamed("avg(precoMaximoDistribuicao)", "avgPrecoMaximoDistribuicao")
      .as[DadosCombustivelAverage]
  }

  def calculateVariance (df: Dataset[DadosCombustivelAverage]) = {
    val list = df.collect.toList
    val ds = list.map(p=> {
      val varianceMinRevenda = Math.pow(p.precoMinimoRevenda - p.avgPrecoMinimoRevenda, 2)
      val varianceMaxRevenda = Math.pow(p.precoMaximoRevenda - p.avgPrecoMaximoRevenda, 2)
      DadosCombustivelVariance(p.monthYear, p.municipio, varianceMinRevenda, varianceMaxRevenda)
    }).toDF

    ds.groupBy("monthYear", "municipio")
    .avg("varianceMinRevenda", "varianceMaxRevenda")
  }

  def calculateVariation (df: Dataset[DadosCombustivelAverage]) = {
    val list = df.collect().toList
    val ds = list.map(p=> {
      val variationMinRevenda = p.precoMaximoRevenda - p.avgPrecoMaximoRevenda
      val variationMaxRevenda = p.precoMinimoRevenda - p.avgPrecoMinimoRevenda
      DadosCombustivelVariation(p.dataInicial, p.dataFinal, p.monthYear, p.regiao, p.estado, p.municipio, p.produto,
        p.numeroDePostosPesquisados, p.unidadeDeMedida, p.precoMedioRevenda, p.desvioPadraoRevenda, p.precoMinimoRevenda, p.precoMaximoRevenda,
        p.margemMediaRevenda, p.coefDeVariaçãoRevenda, p.precoMedioDistribuicao, p.desvioPadraoDistribuicao, p.precoMinimoDistribuicao, p.precoMaximoDistribuicao,
        p.coefDeVariaçãoDistribuicao, p.avgPrecoMedioRevenda, p.avgPrecoMinimoRevenda, p.avgPrecoMaximoRevenda, p.avgMargemMediaRevenda, p.avgPrecoMedioDistribuicao,
        p.avgPrecoMinimoDistribuicao, p.avgPrecoMedioDistribuicao, variationMinRevenda, variationMaxRevenda)
    }).toDF

    ds.groupBy("monthYear", "municipio")
      .avg("variationMinRevenda", "variationMaxRevenda")
  }

  def calculateDifference (df: Dataset[DadosCombustivelAverage]) = {

    val ds = df.groupBy("municipio").agg(max("precoMaximoRevenda")- min("precoMinimoRevenda")as "difference")
    ds.sort(desc("difference")).take(5)
  }

}

