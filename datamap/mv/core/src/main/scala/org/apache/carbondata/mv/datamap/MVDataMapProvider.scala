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
import org.apache.carbondata.core.locks.ICarbonLock
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema, RelationIdentifier}
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.path.CarbonTablePath
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
    DataMapStatusManager.deleteSegmentStatus(dataMapSchema)
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
      val isFullRebuild =
        if (null != dataMapSchema.getProperties.get("full_refresh")) {
          dataMapSchema.getProperties.get("full_refresh").toBoolean
        } else {
          false
        }
      if(isFullRebuild) {
        isOverwriteTable = true
      }
      queryPlan.queryExecution.optimizedPlan transformDown {
        case join@Join(l1, l2, jointype, condition) =>
          // TODO: Support Incremental loading for multiple tables with join - CARBONDATA-3340
          isOverwriteTable = true
          join
      }
      if (!isOverwriteTable && !isFullRebuild) {
        // Set main table segments to load for incremental data loading.
        // Compare main table segments info with datamap table segment map info and load only newly
        // added segment from main table to datamap table
        if (!setSegmentsBasedOnMapping()) {
          return
        }
      }
      val dataMapTable = CarbonTable
        .buildFromTablePath(identifier.getTableName,
          identifier.getDatabaseName,
          identifier.getTablePath,
          identifier.getTableId)
      val header = dataMapTable.getTableInfo.getFactTable.getListOfColumns.asScala
        .filter { column =>
          !column.getColumnName
            .equalsIgnoreCase(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE)
        }.sortBy(_.getSchemaOrdinal).map(_.getColumnName).mkString(",")
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
   * This method will compare mainTable and dataMapTable segment List and loads only newly added
   * segment from main table to dataMap table.
   * In case if mainTable is compacted, then based on dataMap to mainTables segmentMapping, dataMap
   * will be loaded
   *
   * Eg: Consider mainTableSegmentList: {0, 1, 2}, dataMapToMainTable segmentMap: { 0 -> 0, 1-> 1,2}
   * If (1, 2) segments of main table are compacted to 1.1 and new segment (3) is loaded to
   * main table, then mainTableSegmentList will be updated to{0, 1.1, 3}.
   * In this case, segment (1) of dataMap table will be marked for delete, and new segment
   * {2 -> 1.1, 3} will be loaded to dataMap table
   */
  def setSegmentsBasedOnMapping(): Boolean = {
    val relationIdentifiers = dataMapSchema.getParentTables.asScala
    // Get dataMap table to main tables segment map from dataMapSegmentStatus
    val dataMapToMainTableSegmentMap = DataMapStatusManager
      .readDataMapSegmentStatusDetails(dataMapSchema)
    if (dataMapToMainTableSegmentMap.getSegmentMapping.isEmpty) {
      // If segment Map is empty, load all valid segments from main tables to dataMap
      for (relationIdentifier <- relationIdentifiers) {
        val mainTableSegmentList = DataMapStatusManager.getSegmentList(relationIdentifier)
        // If mainTableSegmentList is empty, no need to trigger load command
        // TODO: handle in case of multiple tables load to datamap table
        if (mainTableSegmentList.isEmpty) {
          return false
        }
        setSegmentsToLoadDataMap(relationIdentifier, mainTableSegmentList)
      }
    } else {
      for (relationIdentifier <- relationIdentifiers) {
        val dataMapSegmentMapping = dataMapToMainTableSegmentMap.getSegmentMapping
        // Get all segments for parent relationIdentifier
        val mainTableSegmentList = DataMapStatusManager.getSegmentList(relationIdentifier)
        // Get list of segments of mainTable which are already loaded to MV dataMap table
        val dataMapTableSegmentList = DataMapStatusManager
          .getDataMapSegmentsFromMapping(dataMapToMainTableSegmentMap, relationIdentifier)
        dataMapTableSegmentList.removeAll(mainTableSegmentList)
        mainTableSegmentList.removeAll(DataMapStatusManager
          .getDataMapSegmentsFromMapping(dataMapToMainTableSegmentMap, relationIdentifier))
        if (mainTableSegmentList.isEmpty) {
          return false
        }
        if (!dataMapTableSegmentList.isEmpty) {
          val invalidMainTableSegmentList = new util.ArrayList[String]()
          val validMainTableSegmentList = new util.HashSet[String]()
          val invalidDataMapSegmentList = new util.HashSet[String]()

          // For dataMap segments which are not in main table segment list(if main table
          // is compacted), iterate over those segments and get dataMap segments which needs to
          // be marked for delete and main table segments which needs to be loaded again
          dataMapTableSegmentList.asScala.foreach({ segmentId =>
            val dataMapIterator = dataMapSegmentMapping.entrySet().iterator()
            while (dataMapIterator.hasNext) {
              val dataMap = dataMapIterator.next()
              val dataMapSegment = dataMap.getKey
              val mainTableIterator = dataMap.getValue.entrySet().iterator()
              while (mainTableIterator.hasNext) {
                val iterator = mainTableIterator.next()
                val mainTableName = iterator.getKey
                if (mainTableName.equalsIgnoreCase(relationIdentifier.getTableName)) {
                  val segmentIterator = iterator.getValue
                  if (segmentIterator.contains(segmentId)) {
                    segmentIterator.remove(segmentId)
                    validMainTableSegmentList.addAll(segmentIterator)
                    invalidDataMapSegmentList.add(dataMapSegment)
                    invalidMainTableSegmentList.add(segmentId)
                  }
                }
              }
            }
          })
          // remove invalid segment from validMainTableSegmentList if present
          validMainTableSegmentList.removeAll(invalidMainTableSegmentList)

          val datamapTable = CarbonTable
            .buildFromTablePath(dataMapSchema.getRelationIdentifier.getTableName,
              dataMapSchema.getRelationIdentifier.getDatabaseName,
              dataMapSchema.getRelationIdentifier.getTablePath,
              dataMapSchema.getRelationIdentifier.getTableId)
          val loadMetadataDetails = SegmentStatusManager
            .readLoadMetadata(datamapTable.getMetadataPath)
          var ifTableStatusNeedsToBeUpdated = false
          // Remove invalid dataMap segment list and update dataMap table status file
          for (loadMetadataDetail <- loadMetadataDetails) {
            if (invalidDataMapSegmentList.contains(loadMetadataDetail.getLoadName)) {
              ifTableStatusNeedsToBeUpdated = true
              loadMetadataDetail.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE)
            }
          }
          if (ifTableStatusNeedsToBeUpdated) {
            val segmentStatusManager =
              new SegmentStatusManager(datamapTable.getAbsoluteTableIdentifier)
            val carbonLock: ICarbonLock = segmentStatusManager.getTableStatusLock
            if (carbonLock.lockWithRetries) {
              LOGGER.info(
                "Acquired lock for table" + datamapTable.getDatabaseName + "." + datamapTable
                  .getTableName + " for table status updation")
              SegmentStatusManager.writeLoadDetailsIntoFile(CarbonTablePath
                .getTableStatusFilePath(dataMapSchema.getRelationIdentifier.getTablePath),
                loadMetadataDetails)
            }
            carbonLock.unlock()
          }
          mainTableSegmentList.addAll(validMainTableSegmentList)
          setSegmentsToLoadDataMap(relationIdentifier, mainTableSegmentList)
        } else {
          setSegmentsToLoadDataMap(relationIdentifier, mainTableSegmentList)
        }
      }
    }
    true
  }

  /**
   * This method will set main table segments which needs to be loaded to mv dataMap
   */
  private def setSegmentsToLoadDataMap(relationIdentifier: RelationIdentifier,
      mainTableSegmentList: util.List[String]): Unit = {
    CarbonSession
      .threadSet(CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
                 relationIdentifier.getDatabaseName + "." +
                 relationIdentifier.getTableName,
        mainTableSegmentList.asScala.mkString(","))
  }

  /**
   * This method will set all segments for main table after datamap load
   */
  private def unsetMainTableSegments(): Unit = {
    val relationIdentifiers = dataMapSchema.getParentTables.asScala
    for (relationIdentifier <- relationIdentifiers) {
      CarbonSession
        .threadUnset(CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
                     relationIdentifier.getDatabaseName + "." +
                     relationIdentifier.getTableName)
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
