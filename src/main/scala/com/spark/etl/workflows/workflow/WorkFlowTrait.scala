package com.spark.etl.workflows.workflow

import com.spark.etl.workflows.components.extractors.ExtractorTrait
import com.spark.etl.workflows.components.loaders.LoaderTrait
import com.spark.etl.workflows.components.transformers.TransformTrait
import org.apache.log4j.Logger

import scala.collection.mutable

/**
 * Backbone for Workflow. Maintains all workflow components. Abstracts argument passing and invocation.
 * Maintains the order of components as declared in the Implementation class
 */
trait WorkFlowTrait {

  val log: Logger = Logger.getLogger(this.getClass.getName)

  val extractorsSet: mutable.LinkedHashSet[ExtractorTrait] = mutable.LinkedHashSet[ExtractorTrait]()
  val transformersSet: mutable.LinkedHashSet[TransformTrait] = mutable.LinkedHashSet[TransformTrait]()
  val loadersSet: mutable.LinkedHashSet[LoaderTrait] = mutable.LinkedHashSet[LoaderTrait]()

  def addExtractors(extractors: ExtractorTrait*): Unit = {

    for (ext <- extractors) {
      extractorsSet.add(ext)
    }

  }

  def addTransformers(transformers: TransformTrait*): Unit = {

    for (tran <- transformers) {
      transformersSet.add(tran)
    }

  }

  def addLoaders(loaders: LoaderTrait*): Unit = {

    for (load <- loaders) {
      loadersSet.add(load)
    }

  }
}
