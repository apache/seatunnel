/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package io.github.interestinglab.waterdrop.config

import java.io.File
import java.util.ServiceLoader

import scala.language.reflectiveCalls
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import io.github.interestinglab.waterdrop.apis._

import scala.util.{Failure, Success, Try}
import util.control.Breaks._

class ConfigBuilder(configFile: String) {

  val config = load()

  def load(): Config = {

    // val configFile = System.getProperty("config.path", "")
    if (configFile == "") {
      throw new ConfigRuntimeException("Please specify config file")
    }

    println("[INFO] Loading config file: " + configFile)

    // variables substitution / variables resolution order:
    // config file --> syste environment --> java properties

    Try({
      val config = ConfigFactory
        .parseFile(new File(configFile))
        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
        .resolveWith(ConfigFactory.systemProperties, ConfigResolveOptions.defaults.setAllowUnresolved(true))

      val options: ConfigRenderOptions = ConfigRenderOptions.concise.setFormatted(true)
      println("[INFO] parsed config file: " + config.root().render(options))

      config
    }) match {
      case Success(conf) => conf
      case Failure(exception) => throw new ConfigRuntimeException(exception)
    }
  }

  /**
   * check if config is valid.
   * */
  def checkConfig: Unit = {
    val sparkConfig = this.getSparkConfigs
    val staticInput = this.createStaticInputs("batch")
    val streamingInputs = this.createStreamingInputs("streaming")
    val outputs = this.createOutputs[BaseOutput]("batch")
    val filters = this.createFilters
    val actions = this.createActions
  }

  def getSparkConfigs: Config = {
    config.getConfig("spark")
  }

  def createFilters: List[BaseFilter] = {

    var filterList = List[BaseFilter]()
    config
      .getConfigList("filter")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "filter")

        val obj = Class
          .forName(className)
          .newInstance()
          .asInstanceOf[BaseFilter]

        obj.setConfig(plugin)

        filterList = filterList :+ obj
      })

    filterList
  }

  def createStructuredStreamingInputs(engine: String): List[BaseStructuredStreamingInput] = {

    var inputList = List[BaseStructuredStreamingInput]()
    config
      .getConfigList("input")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "input", engine)

        val obj = Class
          .forName(className)
          .newInstance()

        obj match {
          case inputObject: BaseStructuredStreamingInput => {
            val input = inputObject.asInstanceOf[BaseStructuredStreamingInput]
            input.setConfig(plugin)
            inputList = inputList :+ input
          }
          case _ => // do nothing
        }
      })

    inputList
  }

  def createStreamingInputs(engine: String): List[BaseStreamingInput[Any]] = {

    var inputList = List[BaseStreamingInput[Any]]()
    config
      .getConfigList("input")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "input", engine)

        val obj = Class
          .forName(className)
          .newInstance()

        obj match {
          case inputObject: BaseStreamingInput[Any] => {
            val input = inputObject.asInstanceOf[BaseStreamingInput[Any]]
            input.setConfig(plugin)
            inputList = inputList :+ input
          }
          case _ => // do nothing
        }
      })

    inputList
  }

  def createStaticInputs(engine: String): List[BaseStaticInput] = {

    var inputList = List[BaseStaticInput]()
    config
      .getConfigList("input")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "input", engine)

        val obj = Class
          .forName(className)
          .newInstance()

        obj match {
          case inputObject: BaseStaticInput => {
            val input = inputObject.asInstanceOf[BaseStaticInput]
            input.setConfig(plugin)
            inputList = inputList :+ input
          }
          case _ => // do nothing
        }
      })

    inputList
  }

  def createOutputs[T <: Plugin](engine: String): List[T] = {

    var outputList = List[T]()
    config
      .getConfigList("output")
      .foreach(plugin => {

        val className = engine match {
          case "batch" | "sparkstreaming" =>
            buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "output", "batch")
          case "structuredstreaming" =>
            buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "output", engine)

        }

        val obj = Class
          .forName(className)
          .newInstance()
          .asInstanceOf[T]

        obj.setConfig(plugin)

        outputList = outputList :+ obj
      })

    outputList
  }

  def createActions[T <: Plugin](): List[T] = {

    config.hasPath("action") match {
      case true => createActionsImpl[T]
      case false => List[T]()
    }
  }

  private def createActionsImpl[T <: Plugin](): List[T] = {
    var pluginInstances = List[T]()

    config
      .getConfigList("action")
      .foreach(plugin => {

        val className = buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "action", "")

        val obj = Class
          .forName(className)
          .newInstance()
          .asInstanceOf[T]

        obj.setConfig(plugin)

        pluginInstances = pluginInstances :+ obj
      })

    pluginInstances
  }

  private def getInputType(name: String, engine: String): String = {
    name match {
      case _ if name.toLowerCase.endsWith("stream") => {
        engine match {
          case "batch" => "sparkstreaming"
          case "structuredstreaming" => "structuredstreaming"
        }
      }
      case _ => "batch"
    }
  }

  /**
   * Get full qualified class name by reflection api, ignore case.
   * */
  private def buildClassFullQualifier(name: String, classType: String): String = {
    buildClassFullQualifier(name, classType, "")
  }

  private def buildClassFullQualifier(name: String, classType: String, engine: String): String = {

    var qualifier = name
    if (qualifier.split("\\.").length == 1) {

      val packageName = classType match {
        case "input" => ConfigBuilder.InputPackage + "." + getInputType(name, engine)
        case "filter" => ConfigBuilder.FilterPackage
        case "output" => ConfigBuilder.OutputPackage + "." + engine
        case "action" => ConfigBuilder.ActionPackage
      }

      val services: Iterable[Plugin] =
        (ServiceLoader load classOf[BaseStaticInput]).asScala ++
          (ServiceLoader load classOf[BaseStreamingInput[Any]]).asScala ++
          (ServiceLoader load classOf[BaseFilter]).asScala ++
          (ServiceLoader load classOf[BaseOutput]).asScala ++
          (ServiceLoader load classOf[BaseStructuredStreamingInput]).asScala ++
          (ServiceLoader load classOf[BaseStructuredStreamingOutput]).asScala ++
          (ServiceLoader load classOf[BaseStructuredStreamingOutputIntra]).asScala ++
          (ServiceLoader load classOf[BaseAction]).asScala

      var classFound = false
      breakable {
        for (serviceInstance <- services) {
          val clz = serviceInstance.getClass
          // get class name prefixed by package name
          val clzNameLowercase = clz.getName.toLowerCase()
          val qualifierWithPackage = packageName + "." + qualifier
          if (clzNameLowercase == qualifierWithPackage.toLowerCase) {
            qualifier = clz.getName
            classFound = true
            break
          }
        }
      }
    }

    qualifier
  }
}

object ConfigBuilder {

  val PackagePrefix = "io.github.interestinglab.waterdrop"
  val FilterPackage = PackagePrefix + ".filter"
  val InputPackage = PackagePrefix + ".input"
  val OutputPackage = PackagePrefix + ".output"
  val ActionPackage = PackagePrefix + ".action"

  val PluginNameKey = "plugin_name"
}
