/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.mv.datamap

import java.io.IOException
import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.command.timeseries.TimeSeriesUtil

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException,
  MalformedDataMapCommandException}
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}

class MVTimeSeriesDataMapProvider(mainTable: CarbonTable,
    sparkSession: SparkSession,
    dataMapSchema: DataMapSchema)
  extends MVDataMapProvider(mainTable, sparkSession, dataMapSchema) {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  @throws[MalformedDataMapCommandException]
  @throws[IOException]
  override def initMeta(ctasSqlStatement: String): Unit = {
    if (dataMapSchema.isLazy) {
      throw new MalformedCarbonCommandException(
        "MV TimeSeries datamap does not support Lazy Rebuild")
    }
    val dmProperties: util.Map[String, String] = dataMapSchema.getProperties
    TimeSeriesUtil.validateDMPropertiesForTimeSeries(dmProperties.asScala.toMap)
    super.initMeta(ctasSqlStatement)
  }

}
