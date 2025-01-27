package com.spark.etl.workflows.components.loaders

import com.spark.etl.utils.{SparkIOUtil, TableColumnConstants => TC}
import org.apache.spark.sql.{DataFrame, SaveMode}

class ItemSalesLoader extends LoaderTrait {

  override def load(paramsMap: Map[String, Any], dataFrameMap: Map[String, DataFrame]): Unit = {

    val df = dataFrameMap("itemSalesDF")

    SparkIOUtil.writeOrc(df, SaveMode.Append, TC.itemSalesTable, None)

  }
}
