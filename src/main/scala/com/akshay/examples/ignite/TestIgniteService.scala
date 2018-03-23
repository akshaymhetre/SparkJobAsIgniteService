package com.akshay.examples.ignite

import java.util

import org.apache.ignite.{Ignite, Ignition}
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.services.ServiceConfiguration
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder

object TestIgniteService extends App{
  val configuration = new IgniteConfiguration()
  configuration.setPeerClassLoadingEnabled(true)
  val spi = new TcpDiscoverySpi()

  val finder = new TcpDiscoveryVmIpFinder()
  finder.setAddresses(util.Arrays.asList("192.168.1.98:47500..47510"))
  spi.setIpFinder(finder)
  configuration.setDiscoverySpi(spi)
  configuration

  val ignite: Ignite = Ignition.start(configuration)

  val cfg = new ServiceConfiguration()
  cfg.setName("SparkJobAsService")
  cfg.setTotalCount(1)

  cfg.setService(new SparkJobAsService())

  ignite.services().deployClusterSingleton("SparkJobAsService", new SparkJobAsService())
  //Wait for service to deploy
  Thread.sleep(2000)

  println("Creating proxy")
  val service = ignite.services().serviceProxy("SparkJobAsService", classOf[SparkService], true)
  println("First Run")
  service.run
  println("Second Run")
  service.run

  ignite.close()

}
