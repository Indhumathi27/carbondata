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
import scala.collection.mutable

import org.apache.spark.sql.{CarbonSession, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{Join, SubqueryAlias}
import org.apache.spark.sql.execution.command.management.CarbonLoadDataCommand
import org.apache.spark.sql.execution.command.table.CarbonDropTableCommand
import org.apache.spark.sql.execution.datasources.FindDataSourceTable
import org.apache.spark.sql.parser.CarbonSpark2SqlParser
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.annotations.InterfaceAudience
import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.{DataMapCatalog, DataMapProvider, DataMapStoreManager}
import org.apache.carbondata.core.datamap.dev.{DataMap, DataMapFactory}
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.indexstore.Blocklet
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema, RelationIdentifier}
import org.apache.carbondata.mv.rewrite.{SummaryDataset, SummaryDatasetCatalog}

@InterfaceAudience.Internal
class MVDataMapProvider(
    mainTable: CarbonTable,
    sparkSession: SparkSession,
    dataMapSchema: DataMapSchema)
  extends DataMapProvider(mainTable, dataMapSchema) {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  protected var dropTableCommand: CarbonDropTableCommand = null

  @throws[MalformedDataMapCommandException]
  @throws[IOException]
  override def initMeta(ctasSqlStatement: String): Unit = {
    if (ctasSqlStatement == null) {
      throw new MalformedDataMapCommandException(
        "select statement is mandatory")
    }
    MVHelper.createMVDataMap(sparkSession, dataMapSchema, ctasSqlStatement, true)
    DataMapStoreManager.getInstance.registerDataMapCatalog(this, dataMapSchema)
  }

  override def initData(): Unit = {
  }

  @throws[IOException]
  override def cleanMeta(): Unit = {
    dropTableCommand = new CarbonDropTableCommand(true,
      new Some[String](dataMapSchema.getRelationIdentifier.getDatabaseName),
      dataMapSchema.getRelationIdentifier.getTableName,
      true)
    dropTableCommand.processMetadata(sparkSession)
    DataMapStoreManager.getInstance.unRegisterDataMapCatalog(dataMapSchema)
    DataMapStoreManager.getInstance().dropDataMapSchema(dataMapSchema.getDataMapName)
  }

  override def cleanData(): Unit = {
    if (dropTableCommand != null) {
      dropTableCommand.processData(sparkSession)
    }
  }

  @throws[IOException]
  override def rebuild(): Unit = {
    val ctasQuery = dataMapSchema.getCtasQuery
    if (ctasQuery != null) {
      val identifier = dataMapSchema.getRelationIdentifier
      val logicalPlan =
        new FindDataSourceTable(sparkSession).apply(
          sparkSession.sessionState.catalog.lookupRelation(
          TableIdentifier(identifier.getTableName,
            Some(identifier.getDatabaseName)))) match {
          case s: SubqueryAlias => s.child
          case other => other
        }
      val updatedQuery = new CarbonSpark2SqlParser().addPreAggFunction(ctasQuery)
      val queryPlan = SparkSQLUtil.execute(
        sparkSession.sql(updatedQuery).queryExecution.analyzed,
        sparkSession).drop("preAgg")
      var isOverwriteTable = false
      queryPlan.queryExecution.optimizedPlan transformDown {
        case join@Join(l1, l2, jointype, condition) =>
          isOverwriteTable = true
          join
      }
      if (!isOverwriteTable) {
        // Set main table segments to load for incremental data loading.
        // Compare main table segments info with datamap table info and load only newly added segment
        // from main table to datamap table
        // TODO: handle for join cases
        if (!setMainTableSegmentsToLoad()) {
          return
        }
      }
      val header = logicalPlan.output.map(_.name).mkString(",")
      val loadCommand = CarbonLoadDataCommand(
        databaseNameOp = Some(identifier.getDatabaseName),
        tableName = identifier.getTableName,
        factPathFromUser = null,
        dimFilesPath = Seq(),
        options = scala.collection.immutable.Map("fileheader" -> header),
        isOverwriteTable,
        inputSqlString = null,
        dataFrame = Some(queryPlan),
        updateModel = None,
        tableInfoOp = None,
        internalOptions = Map.empty,
        partition = Map.empty)
      try {
        SparkSQLUtil.execute(loadCommand, sparkSession)
      } catch {
        case ex: Exception =>
          DataMapStatusManager.disableDataMap(dataMapSchema.getDataMapName)
          LOGGER.error("Data Load failed for DataMap: ", ex)
      } finally {
        unsetMainTableSegments()
      }
    }
  }

  /**
   * This method will compare mainTable and datamapTable segment List and loads only newly added
   * segment from main table to datamap table
   */
  private def setMainTableSegmentsToLoad(): Boolean = {
    val relationIdentifiers = dataMapSchema.getParentTables.asScala
    for (relationIdentifier <- relationIdentifiers) {
      val (mainTableSegmentList, datamapTableSegmentList) = getSegmentList(relationIdentifier)
      mainTableSegmentList.removeAll(datamapTableSegmentList)
      if (mainTableSegmentList.isEmpty) {
        return false
      }
      CarbonSession
        .threadSet(CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
                   relationIdentifier.getDatabaseName + "." +
                   relationIdentifier.getTableName,
          mainTableSegmentList.asScala.mkString(","))
    }
    true
  }

  private def getSegmentList(relationIdentifier: RelationIdentifier): (util.ArrayList[String],
    util.ArrayList[String]) = {
    // As MV datamap can contain multiple parent tables, iterate all parentTables to get main
    // table segment list
    val mainTableSegmentList = new util.ArrayList[String](DataMapStatusManager
      .getSegmentList(relationIdentifier))
    val datamapTableSegmentList = new util.ArrayList[String]()
    val dataMapStatusDetails = DataMapStatusManager.readDataMapStatusDetails()
    for (dataMapStatusDetail <- dataMapStatusDetails) {
      if (dataMapStatusDetail.getDataMapName.equals(dataMapSchema.getDataMapName)) {
        datamapTableSegmentList
          .addAll(dataMapStatusDetail.getSegmentInfo.get(relationIdentifier.getTableName))
      }
    }
    (mainTableSegmentList, datamapTableSegmentList)
  }

  /**
   * This method will set all segments for main table after datamap load
   */
  private def unsetMainTableSegments(): Unit = {
    val relationIdentifiers = dataMapSchema.getParentTables.asScala
    for (relationIdentifier <- relationIdentifiers) {
      CarbonSession
        .threadSet(CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
                   relationIdentifier.getDatabaseName + "." +
                   relationIdentifier.getTableName, "*")
    }
  }

  @throws[IOException]
  override def incrementalBuild(
      segmentIds: Array[String]): Unit = {
    throw new UnsupportedOperationException
  }

  override def createDataMapCatalog : DataMapCatalog[SummaryDataset] =
    new SummaryDatasetCatalog(sparkSession)

  override def getDataMapFactory: DataMapFactory[_ <: DataMap[_ <: Blocklet]] = {
    throw new UnsupportedOperationException
  }

  override def supportRebuild(): Boolean = true
}
