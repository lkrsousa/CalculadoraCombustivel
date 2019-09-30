package br.com.empresa.domain

import java.text.SimpleDateFormat
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

case class DadosCombustivel (
                         dataInicial:Date,
                         dataFinal: Date,
                         monthYear: String,
                         regiao: String,
                         estado: String,
                         municipio: String,
                         produto: String,
                         numeroDePostosPesquisados: Int,
                         unidadeDeMedida: String,
                         precoMedioRevenda: Double,
                         desvioPadraoRevenda: Double,
                         precoMinimoRevenda: Double,
                         precoMaximoRevenda: Double,
                         margemMediaRevenda: Option[Double],
                         coefDeVariaçãoRevenda: Double,
                         precoMedioDistribuicao: Option[Double],
                         desvioPadraoDistribuicao: Option[Double],
                         precoMinimoDistribuicao: Option[Double],
                         precoMaximoDistribuicao: Option[Double],
                         coefDeVariaçãoDistribuicao: Option[Double]
                      )

case class DadosCombustivel2 (
                              dataInicial:Date,
                              dataFinal: Date,
                              monthYear: String,
                              regiao: String,
                              estado: String,
                              municipio: String,
                              produto: String,
                              numeroDePostosPesquisados: Int,
                              unidadeDeMedida: String,
                              precoMedioRevenda: Double,
                              desvioPadraoRevenda: Double,
                              precoMinimoRevenda: Double,
                              precoMaximoRevenda: Double,
                              margemMediaRevenda: Option[Double],
                              coefDeVariaçãoRevenda: Double,
                              precoMedioDistribuicao: Option[Double],
                              desvioPadraoDistribuicao: Option[Double],
                              precoMinimoDistribuicao: Option[Double],
                              precoMaximoDistribuicao: Option[Double],
                              coefDeVariaçãoDistribuicao: Option[Double]
                            )

case class DadosCombustivel3 (
                               dataInicial:String,
                               dataFinal: String,
                               regiao: String,
                               estado: String,
                               municipio: String,
                               produto: String,
                               numeroDePostosPesquisados: String,
                               unidadeDeMedida: String,
                               precoMedioRevenda: String,
                               desvioPadraoRevenda: String,
                               precoMinimoRevenda: String,
                               precoMaximoRevenda: String,
                               margemMediaRevenda: String,
                               coefDeVariaçãoRevenda: String,
                               precoMedioDistribuicao: String,
                               desvioPadraoDistribuicao: String,
                               precoMinimoDistribuicao: String,
                               precoMaximoDistribuicao: String,
                               coefDeVariaçãoDistribuicao: String
                             )

object DadosCombustivel {
/*  def mock = {

    implicit val dateFormatter: DateTimeFormatter = DateTimeFormat.forPattern("dd/MM/yyyy")
    val monthFormatter: DateTimeFormatter = DateTimeFormat.forPattern("MM/yyyy")
    val inicio = dateFormatter.parseDateTime("30/12/2018")
    val fim = dateFormatter.parseDateTime("05/01/2019")


    DadosCombustivel (
      inicio.toDate,
      fim.toDate,
      s"""${this.getMin(this.getMonthByDate(inicio), this.getMonthByDate(fim))}/${this.getMin(this.getYearByDate(inicio), this.getMonthByDate(fim))}""",
      "NORTE",
      "PARA",
      "ABAETETUBA",
      "ETANOL HIDRATADO",
      1,
      "R$/l",
      5,
      0,
      5,
      5,
      None,
      0,
      None,
      None,
      None,
      None,
      None
    )
  }*/

  def dateFormat(date: String) = {
    val dateFormat = new SimpleDateFormat("dd/MM/yyyy")
    val parsedDate = dateFormat.parse(date)
    new Date(parsedDate.getTime)
  }

  def getMonthByDate(date: Date) = {
    date.toLocalDate.getMonthValue
  }

  def getYearByDate(date: Date) = {
    date.toLocalDate.getYear
  }

  def getMin(start: Int, end: Int) = {
    if(start < end) start else end
  }

  def parseTo (p: DadosCombustivel3)= {
    val start = dateFormat(p.dataInicial)
    val end = dateFormat(p.dataFinal)
    val date = if(start.toLocalDate.isBefore(end.toLocalDate)){start} else {end}
    val montYear = s"""${this.getMonthByDate(date)}/${this.getYearByDate(date)}"""
    DadosCombustivel2(
      start,
      end,
      montYear,
      p.regiao,
      p.estado,
      p.municipio,
      p.produto,
      p.numeroDePostosPesquisados.toInt,
      p.unidadeDeMedida,
      p.precoMedioRevenda.replace(",", ".").toDouble,
      p.desvioPadraoRevenda.replace(",", ".").toDouble,
      p.precoMinimoRevenda.replace(",", ".").toDouble,
      p.precoMaximoRevenda.replace(",", ".").toDouble,
      try {
        Some(p.margemMediaRevenda.replace(",", ".").toDouble)
      } catch {
        case e: Exception => None
      },
      p.coefDeVariaçãoRevenda.replace(",", ".").toDouble,
      try {
        Some(p.precoMedioDistribuicao.replace(",", ".").toDouble)
      } catch {
        case e: Exception => None
      },
      try {
        Some(p.desvioPadraoDistribuicao.replace(",", ".").toDouble)
      } catch {
        case e: Exception => None
      },
      try {
        Some(p.precoMinimoDistribuicao.replace(",", ".").toDouble)
      } catch {
        case e: Exception => None
      },
      try {
        Some(p.precoMaximoDistribuicao.replace(",", ".").toDouble)
      } catch {
        case e: Exception => None
      },
      try {
        Some(p.coefDeVariaçãoDistribuicao.replace(",", ".").toDouble)
      } catch {
        case e: Exception => None
      }
    )
  }

}
