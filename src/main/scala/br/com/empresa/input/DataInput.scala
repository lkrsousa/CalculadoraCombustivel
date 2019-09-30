package br.com.empresa.input

import br.com.empresa.domain.{DadosCombustivel, DadosCombustivel2, DadosCombustivel3}
import br.com.empresa.settings.Settings
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class DataInput {

  def loadCSV(ss: SparkSession) = {
    import ss.implicits._
  ss.read.format("csv").option("header", "true").load(Settings.path)
  }

}
