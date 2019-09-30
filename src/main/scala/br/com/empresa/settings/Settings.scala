package br.com.empresa.settings

import com.typesafe.config.ConfigFactory

object Settings {

  private val config = ConfigFactory.load()
  private val hadoopSettings = config.getConfig("hadoop_settings")
  private val weblogGen = config.getConfig("calculadora-combustivel")
  lazy val winUtils: String = hadoopSettings.getString("winUtils")
  lazy val appName: String = weblogGen.getString("appName")
  lazy val path: String = weblogGen.getString("locationPath")




}
