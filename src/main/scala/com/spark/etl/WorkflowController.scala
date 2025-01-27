package com.spark.etl

import com.spark.etl.utils.{ConfigUtil, SparkIOUtil, StringConstantsUtil}
import org.apache.log4j.Logger

/**
 * Controller - configures Spark, loads configurations and delegates control to
 * workflow manager
 *
 */

object WorkflowController {

  val log: Logger = Logger.getLogger(this.getClass.getName)
  var paramsMap: Map[String, Any] = scala.collection.immutable.Map()

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {

    } else {
      for (arg <- args) {
        val res = arg.split("=")
        paramsMap = paramsMap + (res(0) -> res(1))
      }
    }

    configureSpark(paramsMap)

    WorkFlowManager.manageWorkFlow(paramsMap)
  }

  private def configureSpark(paramsMap: Map[String, Any]): Unit = {

    val allParams = paramsMap ++ ConfigUtil.getGlobalConfig(paramsMap.get(StringConstantsUtil.RUNMODE)) ++
      ConfigUtil.getAppConfig(paramsMap(StringConstantsUtil.WORKFLOW).toString)

    SparkIOUtil.configureSpark(allParams)

    log.debug("Spark is configured")
  }


}
