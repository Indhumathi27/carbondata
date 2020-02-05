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

package org.apache.spark.sql.secondaryindex.joins

import java.io.IOException
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.{Job, JobContext}
import org.apache.log4j.Logger
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, BindReferences, Expression, In, Literal}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.{BinaryExecNode, ProjectExec, RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide, HashJoin}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.strategy.CarbonDataSourceScan
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.util.SparkSQLUtil
import org.apache.spark.unsafe.types.UTF8String

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.{DataMapFilter, DataMapStoreManager, DistributableDataMapFormat, Segment}
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.readcommitter.ReadCommittedScope
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager, SegmentUpdateStatusManager}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.hadoop.api.{CarbonInputFormat, CarbonTableInputFormat}
import org.apache.carbondata.indexserver.IndexServer
import org.apache.carbondata.spark.rdd.CarbonScanRDD

case class BroadCastSIFilterPushJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    left: SparkPlan,
    right: SparkPlan,
    condition: Option[Expression]) extends BinaryExecNode with HashJoin {

  val logger: Logger = LogServiceFactory.getLogService(this.getClass.getName)

  override def output: Seq[Attribute] = carbonScan.output


  override lazy val metrics = Map(
    "numLeftRows" -> SQLMetrics.createMetric(sparkContext, "number of left rows"),
    "numRightRows" -> SQLMetrics.createMetric(sparkContext, "number of right rows"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))
  lazy val partitions: Array[Segment] = if (mainTableRDD.isDefined && mainTableRDD.get
    .isInstanceOf[CarbonScanRDD[InternalRow]]) {
    getFilteredSegments(mainTableRDD.get.asInstanceOf[CarbonScanRDD[InternalRow]])
  } else {
    Array.empty[Segment]
  }
  private lazy val (input: Array[InternalRow], inputCopy: Array[InternalRow]) = {
    val numBuildRows = buildSide match {
      case BuildLeft => longMetric("numLeftRows")
      case BuildRight => longMetric("numRightRows")
    }
    val secondaryIndexRDD = buildPlan.collect {
      case batchData: CarbonDataSourceScan =>
        batchData.rdd
      case rowData: RowDataSourceScanExec =>
        rowData.rdd
    }
    if (partitions.nonEmpty && secondaryIndexRDD.nonEmpty) {
      secondaryIndexRDD.foreach {
        case value: CarbonScanRDD[InternalRow] =>
          value.setSegmentsToAccess(partitions)
        case _ =>
      }
    }
    // If the partitions that are recognized from the main table are empty then no need to
    // execute the SI plan.
    if (partitions.nonEmpty) {
      val input: Array[InternalRow] = buildPlan.execute.map(_.copy()).collect()
      val inputCopy: Array[InternalRow] = input.clone()
      (input, inputCopy)
    } else {
      (Array.empty[InternalRow], Array.empty[InternalRow])
    }
  }
  val carbonScan: SparkPlan = buildSide match {
    case BuildLeft => right
    case BuildRight => left
  }
  val mainTableRDD: Option[RDD[InternalRow]] = carbonScan.collectFirst {
    case batchData: CarbonDataSourceScan =>
      batchData.rdd
    case rowData: RowDataSourceScanExec =>
      rowData.rdd
  }

  /**
   * This method is used to get the valid segments for the query based on the filter condition.
   *
   * @return Array of valid segments
   */
  def getFilteredSegments(carbonScanRdd: CarbonScanRDD[InternalRow]): Array[Segment] = {
    val LOGGER = LogServiceFactory.getLogService(BroadCastSIFilterPushJoin.getClass.getName)
    val conf = new Configuration()
    val jobConf = new JobConf(conf)
    SparkHadoopUtil.get.addCredentials(jobConf)
    val job = Job.getInstance(jobConf)
    val format = carbonScanRdd.prepareInputFormatForDriver(job.getConfiguration)
    val startTime = System.currentTimeMillis()
    val segmentsToAccess: Array[Segment] = BroadCastSIFilterPushJoin
      .getFilteredSegments(job, format)
      .asScala
      .toArray
    LOGGER
      .info("Time taken for getting the splits: " + (System.currentTimeMillis - startTime) +
            " ,Total split: " + segmentsToAccess.length)
    segmentsToAccess
  }

  override def doExecute(): RDD[InternalRow] = {
    addInFilterToPlan(buildPlan,
      carbonScan,
      inputCopy,
      leftKeys,
      rightKeys,
      buildSide,
      isIndexTable = true)
    carbonScan.execute
  }

  def addInFilterToPlan(buildPlan: SparkPlan,
      carbonScan: SparkPlan,
      inputCopy: Array[InternalRow],
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      buildSide: BuildSide,
      isIndexTable: Boolean = false): Unit = {

    val keys = {
      buildSide match {
        case BuildLeft => (leftKeys)
        case BuildRight => (rightKeys)
      }
      }.map { a =>
      BindReferences.bindReference(a, buildPlan.output)
    }.toArray

    val filters = keys.map {
      k =>
        inputCopy.map(
          r => {
            val curr = k.eval(r)
            curr match {
              case _: UTF8String => Literal(curr.toString).asInstanceOf[Expression]
              case _: Long if k.dataType.isInstanceOf[TimestampType] =>
                Literal(curr, TimestampType).asInstanceOf[Expression]
              case _ => Literal(curr).asInstanceOf[Expression]
            }
          })
    }

    val filterKey = (buildSide match {
      case BuildLeft => rightKeys
      case BuildRight => leftKeys
    }).collectFirst { case a: Attribute => a }

    def resolveAlias(expressions: Seq[Expression]) = {
      val aliasMap = new mutable.HashMap[Attribute, Expression]()
      carbonScan.transformExpressions {
        case alias: Alias =>
          aliasMap.put(alias.toAttribute, alias.child)
          alias
      }
      expressions.map {
        case at: AttributeReference =>
          // cannot use Map.get() as qualifier is different.
          aliasMap.find(_._1.semanticEquals(at)) match {
            case Some(child) => child._2
            case _ => at
          }
        case others => others
      }
    }

    val filterKeys = buildSide match {
      case BuildLeft =>
        resolveAlias(rightKeys)
      case BuildRight =>
        resolveAlias(leftKeys)
    }

    val tableScan = carbonScan.collectFirst {
      case ProjectExec(projectList, batchData: CarbonDataSourceScan)
        if (filterKey.isDefined && (isIndexTable || projectList.exists(x =>
          x.name.equalsIgnoreCase(filterKey.get.name) &&
          x.exprId.id == filterKey.get.exprId.id &&
          x.exprId.jvmId.equals(filterKey.get.exprId.jvmId)))) =>
        batchData
      case ProjectExec(projectList, rowData: RowDataSourceScanExec)
        if (filterKey.isDefined && (isIndexTable || projectList.exists(x =>
          x.name.equalsIgnoreCase(filterKey.get.name) &&
          x.exprId.id == filterKey.get.exprId.id &&
          x.exprId.jvmId.equals(filterKey.get.exprId.jvmId)))) =>
        rowData
      case batchData: CarbonDataSourceScan
        if (filterKey.isDefined && (isIndexTable || batchData.output.attrs.exists(x =>
          x.name.equalsIgnoreCase(filterKey.get.name) &&
          x.exprId.id == filterKey.get.exprId.id &&
          x.exprId.jvmId.equals(filterKey.get.exprId.jvmId)))) =>
        batchData
      case rowData: RowDataSourceScanExec
        if (filterKey.isDefined && (isIndexTable || rowData.output.exists(x =>
          x.name.equalsIgnoreCase(filterKey.get.name) &&
          x.exprId.id == filterKey.get.exprId.id &&
          x.exprId.jvmId.equals(filterKey.get.exprId.jvmId)))) =>
        rowData
    }
    val configuredFilterRecordSize = CarbonProperties.getInstance.getProperty(
      CarbonCommonConstants.BROADCAST_RECORD_SIZE,
      CarbonCommonConstants.DEFAULT_BROADCAST_RECORD_SIZE)

    if (tableScan.isDefined && null != filters
        && filters.length > 0
        && ((filters(0).length > 0 && filters(0).length <= configuredFilterRecordSize.toInt) ||
            isIndexTable)) {
      logger.info("Pushing down filter for broadcast join. Filter size:" + filters(0).length)
      tableScan.get match {
        case scan: CarbonDataSourceScan =>
          addPushdownToCarbonRDD(scan.rdd,
            addPushdownFilters(filterKeys, filters))
        case _ =>
          addPushdownToCarbonRDD(tableScan.get.asInstanceOf[RowDataSourceScanExec].rdd,
            addPushdownFilters(filterKeys, filters))
      }
    }
  }

  private def addPushdownToCarbonRDD(rdd: RDD[InternalRow],
      expressions: Seq[Expression]): Unit = {
    rdd match {
      case value: CarbonScanRDD[InternalRow] =>
        if (expressions.nonEmpty) {
          val expressionVal = CarbonFilters
            .transformExpression(CarbonFilters.preProcessExpressions(expressions).head)
          if (null != expressionVal) {
            value.setFilterExpression(expressionVal)
          }
        }
      case _ =>
    }
  }

  private def addPushdownFilters(keys: Seq[Expression],
      filters: Array[Array[Expression]]): Seq[Expression] = {

    // TODO Values in the IN filter is duplicate. replace the list with set
    val buffer = new ArrayBuffer[Expression]
    keys.zipWithIndex.foreach { a =>
      buffer += In(a._1, filters(a._2)).asInstanceOf[Expression]
    }

    // Let's not pushdown condition. Only filter push down is sufficient.
    // Conditions can be applied on hash join result.
    val cond = if (buffer.size > 1) {
      val e = buffer.remove(0)
      buffer.fold(e)(And)
    } else {
      buffer.asJava.get(0)
    }
    Seq(cond)
  }
}

object BroadCastSIFilterPushJoin {

  val logger: Logger = LogServiceFactory.getLogService(this.getClass.getName)

  /**
   * Used to get the valid segments after applying the following conditions.
   * 1. if user has specified segments for the parent table then those segments would be
   * considered
   * and valid segments would be filtered.
   * 2. if user has not specified segments then all valid segments would be considered for
   * scanning.
   *
   * @param job
   * @return
   * @throws IOException
   */
  def getFilteredSegments(job: JobContext,
      carbonTableInputFormat: CarbonTableInputFormat[Object]): util.List[Segment] = {
    val carbonTable: CarbonTable = carbonTableInputFormat.getOrCreateCarbonTable(job
      .getConfiguration)
    // this will be null in case of corrupt schema file.
    if (null == carbonTable) {
      throw new IOException("Missing/Corrupt schema file for table.")
    } // copy dynamic set segment property from parent table to child index table
    setQuerySegmentForIndexTable(job.getConfiguration, carbonTable)
    val identifier: AbsoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier
    val readCommittedScope: ReadCommittedScope = carbonTableInputFormat.getReadCommitted(job,
      identifier)
    val segmentsToAccess: Array[Segment] = carbonTableInputFormat.getSegmentsToAccess(job,
      readCommittedScope)
    val segmentsToAccessSet: util.Set[Segment] = new util.HashSet[Segment]
    for (segId <- segmentsToAccess) {
      segmentsToAccessSet.add(segId)
    }
    // get all valid segments and set them into the configuration
    val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(identifier)
    val segments: SegmentStatusManager.ValidAndInvalidSegmentsInfo = segmentStatusManager
      .getValidAndInvalidSegments(carbonTable.isChildTableForMV)
    val validSegments: util.List[Segment] = segments.getValidSegments
    // if no segments in table
    val validSegmentsToAccess: util.List[Segment] = new util.ArrayList[Segment]
    if (validSegments.size == 0) {
      return new util.ArrayList[Segment](0)
    }
    if (segmentsToAccess.length == 0 ||
        segmentsToAccess(0).getSegmentNo.equalsIgnoreCase("*")) {
      validSegmentsToAccess.addAll(
        validSegments)
    } else {
      val filteredSegmentToAccess: util.List[Segment] = new util.ArrayList[Segment]
      for (segment <- validSegments.asScala) {
        if (segmentsToAccessSet.contains(segment)) {
          filteredSegmentToAccess.add(segment)
        }
      }
      if (!filteredSegmentToAccess.containsAll(segmentsToAccessSet)) {
        val filteredSegmentToAccessTemp: util.List[Segment] = new util.ArrayList[Segment]
        filteredSegmentToAccessTemp.addAll(filteredSegmentToAccess)
        filteredSegmentToAccessTemp.removeAll(segmentsToAccessSet)
        logger.info(
          "Segments ignored are : " + util.Arrays.toString(filteredSegmentToAccessTemp.toArray))
      }
      // if no valid segments after filteration
      if (filteredSegmentToAccess.size == 0) {
        return new util.ArrayList[Segment](0)
      } else {
        validSegmentsToAccess.addAll(filteredSegmentToAccess)
      }
    }
    CarbonInputFormat.setSegmentsToAccess(job.getConfiguration, validSegmentsToAccess)
    //    return getSplitsInternal(job, true);
    // process and resolve the expression
    val filter: DataMapFilter = carbonTableInputFormat.getFilterPredicates(job.getConfiguration)
    filter.processFilterExpression(null, null)
    val filterInterface: FilterResolverIntf = filter.getResolver
    val filteredSegments: util.List[Segment] = new util.ArrayList[Segment]
    if (filter != null) { // refresh the segments if needed
      val loadMetadataDetails: Array[LoadMetadataDetails] = readCommittedScope.getSegmentList
      val updateStatusManager: SegmentUpdateStatusManager = new SegmentUpdateStatusManager(
        carbonTable,
        loadMetadataDetails)
      val setSegID: util.List[Segment] = new util.ArrayList[Segment]
      if (CarbonProperties.getInstance
        .isDistributedPruningEnabled(carbonTable.getDatabaseName, carbonTable.getTableName)) {
        val segmentsToBeRefreshed: util.List[String] = DataMapStoreManager.getInstance
          .getSegmentsToBeRefreshed(carbonTable, updateStatusManager, validSegmentsToAccess)
        try {
          val dataMapFormat: DistributableDataMapFormat =
            new DistributableDataMapFormat(carbonTable,
              filterInterface,
              validSegmentsToAccess,
              segmentsToBeRefreshed,
              null,
              false,
              null,
              false, false)
          dataMapFormat.setTaskGroupId(SparkSQLUtil.getTaskGroupId(SparkSQLUtil.getSparkSession))
          dataMapFormat.setTaskGroupDesc(SparkSQLUtil.getTaskGroupDesc(SparkSQLUtil
            .getSparkSession))
          setSegID.addAll(IndexServer.getClient.getPrunedSegments(dataMapFormat).getSegments)
        } catch {
          case e: Exception =>
            logger.warn("Distributed Segment Pruning failed, initiating embedded pruning", e)
            try {
              val dataMapFormat: DistributableDataMapFormat = new DistributableDataMapFormat(
                carbonTable,
                filterInterface,
                validSegmentsToAccess,
                segmentsToBeRefreshed,
                null,
                false,
                null,
                true, false)
              setSegID.addAll(IndexServer.getPrunedSegments(dataMapFormat).getSegments)
              val segmentsToBeCleaned: Array[String] = new Array[String](validSegments.size)
              for (i <- 0 until validSegments.size) {
                segmentsToBeCleaned(i) = validSegments.get(i).getSegmentNo
              }
              IndexServer.invalidateSegmentCache(carbonTable,
                segmentsToBeCleaned,
                SparkSQLUtil.getTaskGroupId(SparkSQLUtil.getSparkSession))
            } catch {
              case ex: Exception =>
                logger.warn("Embedded Segment Pruning failed, initiating driver pruning", ex)
                DataMapStoreManager.getInstance
                  .refreshSegmentCacheIfRequired(carbonTable,
                    updateStatusManager,
                    validSegmentsToAccess)
                val blockletMap = DataMapStoreManager.getInstance.getDefaultDataMap(carbonTable)
                return blockletMap.pruneSegments(filteredSegments, filterInterface)
            }
        }
      } else {
        DataMapStoreManager.getInstance
          .refreshSegmentCacheIfRequired(carbonTable, updateStatusManager, validSegmentsToAccess)
        val blockletMap = DataMapStoreManager.getInstance.getDefaultDataMap(carbonTable)
        return blockletMap.pruneSegments(filteredSegments, filterInterface)
      }
      filteredSegments.addAll(setSegID)
    } else {
      filteredSegments.addAll(validSegmentsToAccess)
    }
    filteredSegments
  }

  /**
   * To copy dynamic set segment property form parent table to index table
   */
  def setQuerySegmentForIndexTable(conf: Configuration, carbonTable: CarbonTable) {
    if (carbonTable.isIndexTable) {
      val dbName = carbonTable.getDatabaseName;
      val tbName = carbonTable.getSIParentName;
      val segmentNumbersFromProperty = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_INPUT_SEGMENTS + dbName + "." + tbName, "*");
      if (!segmentNumbersFromProperty.trim().equals("*")) {
        CarbonInputFormat.setSegmentsToAccess(conf,
          Segment.toSegmentList(segmentNumbersFromProperty.split(","), null))
      }
    }
  }
}
