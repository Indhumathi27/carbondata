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

package org.apache.spark.sql.execution.command.mv

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.events._


object MVListeners {
  def getDataMapTableColumns(dataMapSchema: DataMapSchema): mutable.Buffer[String] = {
    val datamapTable = CarbonTable
      .buildFromTablePath(dataMapSchema.getRelationIdentifier.getTableName,
        dataMapSchema.getRelationIdentifier.getDatabaseName,
        dataMapSchema.getRelationIdentifier.getTablePath,
        dataMapSchema.getRelationIdentifier.getTableId)
    val listOfColumns = datamapTable.getTableInfo.getFactTable.getListOfColumns.asScala
      .map(f => f.getColumnName.substring(f.getColumnName.indexOf("_") + 1))
    listOfColumns
  }

  def isMVdatamapTable(carbonTable: CarbonTable): Boolean = {
    val isMVdatamapTable = carbonTable.getTableInfo.getFactTable.getTableProperties
      .get("isMVdatamapTable")
    if (isMVdatamapTable != null && isMVdatamapTable.equals("true")) {
      return true
    }
    false
  }
}

/**
 * Listeners to block operations like delete segment on id or by date on tables
 * having an mv datamap or on mv datamap tables
 */
object MVDeleteSegmentPreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val carbonTable = event match {
      case e: DeleteSegmentByIdPreEvent =>
        e.asInstanceOf[DeleteSegmentByIdPreEvent].carbonTable
      case e: DeleteSegmentByDatePreEvent =>
        e.asInstanceOf[DeleteSegmentByDatePreEvent].carbonTable
    }
    if (null != carbonTable) {
      if (CarbonTable.hasMVDataMap(carbonTable)) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on tables which have mv datamap")
      }
      if (MVListeners.isMVdatamapTable(carbonTable)) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on mv table")
      }
    }
  }
}

object MVAddColumnsPreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val dataTypeChangePreListener = event.asInstanceOf[AlterTableAddColumnPreEvent]
    val carbonTable = dataTypeChangePreListener.carbonTable
    if (MVListeners.isMVdatamapTable(carbonTable)) {
      throw new UnsupportedOperationException(
        s"Cannot add columns in MV DataMap table ${
          carbonTable.getDatabaseName
        }.${ carbonTable.getTableName }")
    }
  }
}


object MVDropColumnPreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val dropColumnChangePreListener = event.asInstanceOf[AlterTableDropColumnPreEvent]
    val carbonTable = dropColumnChangePreListener.carbonTable
    val alterTableDropColumnModel = dropColumnChangePreListener.alterTableDropColumnModel
    val columnsToBeDropped = alterTableDropColumnModel.columns
    if (CarbonTable.hasMVDataMap(carbonTable)) {
      val dataMapSchemaList = DataMapStoreManager.getInstance
        .getDataMapSchemasOfTable(carbonTable).asScala
      for (dataMapSchema <- dataMapSchemaList) {
        if (dataMapSchema.getProviderName.equalsIgnoreCase(DataMapClassProvider.MV.getShortName)) {
          val listOfColumns = MVListeners.getDataMapTableColumns(dataMapSchema)
          val columnExistsInChild = listOfColumns.collectFirst {
            case parentColumnName if columnsToBeDropped.contains(parentColumnName) =>
              parentColumnName
          }
          if (columnExistsInChild.isDefined) {
            throw new UnsupportedOperationException(
              s"Column ${ columnExistsInChild.head } cannot be dropped because it exists " +
              s"in mv datamap ${ dataMapSchema.getRelationIdentifier.toString }")
          }
        }
      }
    }
    if (MVListeners.isMVdatamapTable(carbonTable)) {
      throw new UnsupportedOperationException(
        s"Cannot drop columns present in MV datamap table ${ carbonTable.getDatabaseName }." +
        s"${ carbonTable.getTableName }")
    }
  }
}

object MVChangeDataTypeorRenameColumnPreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val colRenameDataTypeChangePreListener = event
      .asInstanceOf[AlterTableColRenameAndDataTypeChangePreEvent]
    val carbonTable = colRenameDataTypeChangePreListener.carbonTable
    val alterTableDataTypeChangeModel = colRenameDataTypeChangePreListener
      .alterTableDataTypeChangeModel
    val columnToBeAltered: String = alterTableDataTypeChangeModel.columnName
    if (CarbonTable.hasMVDataMap(carbonTable)) {
      val dataMapSchemaList = DataMapStoreManager.getInstance
        .getDataMapSchemasOfTable(carbonTable).asScala
      for (dataMapSchema <- dataMapSchemaList) {
        if (dataMapSchema.getProviderName.equalsIgnoreCase(DataMapClassProvider.MV.getShortName)) {
          val listOfColumns = MVListeners.getDataMapTableColumns(dataMapSchema)
          if (listOfColumns.contains(columnToBeAltered)) {
            throw new UnsupportedOperationException(
              s"Column $columnToBeAltered exists in a MV datamap. Drop MV datamap to " +
              s"continue")
          }
        }
      }
    }
    if (MVListeners.isMVdatamapTable(carbonTable)) {
      throw new UnsupportedOperationException(
        s"Cannot change data type or rename column for columns present in mv datamap table ${
          carbonTable.getDatabaseName
        }.${ carbonTable.getTableName }")
    }
  }
}

