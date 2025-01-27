package com.spark.etl.workflows.workflow

import com.spark.etl.workflows.components.extractors.{SalesExtractor, StoreExtractor}
import com.spark.etl.workflows.components.loaders.StoreSalesLoader
import com.spark.etl.workflows.components.transformers.StoreSalesTransformer



object StoreSalesWorkflow extends WorkFlowTrait{

  addExtractors(new SalesExtractor, new StoreExtractor)

  addTransformers(new StoreSalesTransformer)

  addLoaders(new StoreSalesLoader)

}
