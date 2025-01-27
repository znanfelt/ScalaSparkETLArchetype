package com.spark.etl.workflows.components.loaders

import com.spark.etl.utils.{SparkIOUtil, TableColumnConstants => TC}
import org.apache.spark.sql.{DataFrame, SaveMode}

class StoreSalesLoader extends LoaderTrait {
  override def load(paramsMap: Map[String, Any], dataFrameMap: Map[String, DataFrame]):
  Unit = {

    val df = dataFrameMap("storeSalesDF")

    SparkIOUtil.writeOrc(df, SaveMode.Append, TC.storeSalesTable, None)

  }
}
