package br.com.empresa.domain

import java.sql.Date

case class DadosCombustivelVariance (monthYear: String,
                                     municipio: String,
                                     varianceMinRevenda: Double,
                                     varianceMaxRevenda: Double
                                   )

case class DadosCombustivelVariation (
                                      dataInicial: Date,
                                      dataFinal: Date,
                                      monthYear: String,
                                      regiao: String,
                                      estado: String,
                                      municipio: String,
                                      produto: String,
                                      numeroDePostosPesquisados: Integer,
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
                                      coefDeVariaçãoDistribuicao: Option[Double],
                                      avgPrecoMedioRevenda: Double,
                                      avgPrecoMinimoRevenda: Double,
                                      avgPrecoMaximoRevenda: Double,
                                      avgMargemMediaRevenda: Option[Double],
                                      avgPrecoMedioDistribuicao: Option[Double],
                                      avgPrecoMinimoDistribuicao: Option[Double],
                                      avgPrecoMaximoDistribuicao: Option[Double],
                                      variationMinRevenda: Double,
                                      variationMaxRevenda: Double
                                    )

case class DadosCombustivelDifference (municipio: String,
                                       differenceRevenda: Double
                                     )