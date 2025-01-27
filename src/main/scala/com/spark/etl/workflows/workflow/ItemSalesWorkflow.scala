package com.spark.etl.workflows.workflow

import com.spark.etl.workflows.components.extractors.{ItemExtractor, SalesExtractor}
import com.spark.etl.workflows.components.loaders.ItemSalesLoader
import com.spark.etl.workflows.components.transformers.ItemSalesTransformer

object ItemSalesWorkflow extends WorkFlowTrait {

  addExtractors(new SalesExtractor, new ItemExtractor)

  addTransformers(new ItemSalesTransformer)

  addLoaders(new ItemSalesLoader)

}
