package br.com.empresa.spark

import br.com.empresa.settings.Settings
import org.apache.log4j.Level
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Spark extends Serializable {
  System.setProperty("hadoop.home.dir", Settings.winUtils)

  val session: SparkSession = this.setSession()

  def setLogLevel(level: Level): Unit = {
    this.getContext.setLogLevel(level.toString)
  }

  def setSession(): SparkSession = {
    val conf: SparkConf = new SparkConf()

    SparkSession
      .builder()
      .config(conf)
      .appName(Settings.appName)
      .getOrCreate()
  }

  def getSession: SparkSession = {
    this.session
  }

  def getContext: SparkContext = {
    this.getSession.sparkContext
  }

  def getContextConf: SparkConf = {
    this.getContext.getConf
  }

  def stop(): Unit = {

    this.getContext.stop()
    this.getSession.stop()
  }
}