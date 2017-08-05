package org.interestinglab.waterdrop.config

import scala.language.reflectiveCalls
import scala.collection.JavaConversions._
import com.typesafe.config.Config
import org.interestinglab.waterdrop.input.BaseInput
import org.interestinglab.waterdrop.output.BaseOutput
import org.interestinglab.waterdrop.sql.BaseSQL
import org.interestinglab.waterdrop.filter.BaseFilter
import org.interestinglab.waterdrop.input.BaseInput
import org.interestinglab.waterdrop.output.BaseOutput
import org.interestinglab.waterdrop.sql.BaseSQL
import org.interestinglab.waterdrop.filter.BaseFilter
import org.interestinglab.waterdrop.input.BaseInput
import org.interestinglab.waterdrop.output.BaseOutput
import org.interestinglab.waterdrop.sql.BaseSQL


class ConfigBuilder(val config: Config) {

    def createFilters() : List[BaseFilter] = {

        val filterConf = config.getConfig("filter")
        val filterConfObj = config.getObject("filter")
        var filterList = List[BaseFilter]()

        filterConfObj.foreach({ case (k, v) =>

            var className = k
            if (k.split("\\.").length == 1) {
                className = ConfigBuilder.FilterPackage + "." + className.capitalize
            }

            val filterObj = Class.forName(className)
                .getConstructor(classOf[Config])
                .newInstance(filterConf.getConfig(k))
                .asInstanceOf[BaseFilter]

            filterList = filterList :+ filterObj
        })

        filterList
    }

    def createSQLs() : List[BaseSQL] = {

        val sqlConf = config.getConfig("sql")
        val sqlConfObj = config.getObject("sql")
        var sqlList = List[BaseSQL]()

        sqlConfObj.foreach({ case (k, v) =>

            var className = k
            if (k.split("\\.").length == 1) {
                className = ConfigBuilder.SQLPackage + "." + className.capitalize
            }

            val sqlObj = Class.forName(className)
                .getConstructor(classOf[Config])
                .newInstance(sqlConf.getConfig(k))
                .asInstanceOf[BaseSQL]

                sqlList = sqlList :+ sqlObj
        })

        sqlList
    }

    def createInputs() : List[BaseInput] = {

        val inputConf = config.getConfig("input")
        val inputConfObj = config.getObject("input")
        var inputList = List[BaseInput]()

        inputConfObj.foreach({ case (k, v) =>

            var className = k
            if (k.split("\\.").length == 1) {
                className = ConfigBuilder.InputPackage + "." + className.capitalize
            }

            val obj = Class.forName(className)
                .getConstructor(classOf[Config])
                .newInstance(inputConf.getConfig(k))
                .asInstanceOf[BaseInput]

            inputList = inputList :+ obj
        })

        inputList
    }

    def createOutputs() : List[BaseOutput] = {

        val outputConf = config.getConfig("output")
        val outputConfObj = config.getObject("output")
        var outputList = List[BaseOutput]()

        outputConfObj.foreach({ case (k, v) =>

            var className = k
            if (k.split("\\.").length == 1) {
                className = ConfigBuilder.OutputPackage + "." + className.capitalize
            }

            val obj = Class.forName(className)
                .getConstructor(classOf[Config])
                .newInstance(outputConf.getConfig(k))
                .asInstanceOf[BaseOutput]

            outputList = outputList :+ obj
        })

        outputList
    }
}

object ConfigBuilder {

    val PackagePrefix = "com.lecloud.oi.streamingetl"
    val FilterPackage = PackagePrefix + ".filter"
    val InputPackage = PackagePrefix + ".input"
    val OutputPackage = PackagePrefix + ".output"
    val SQLPackage = PackagePrefix + ".sql"

    def apply(config : Config) = {
        new ConfigBuilder(config)
    }
}
