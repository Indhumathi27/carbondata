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

import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.events._

object MVDeleteSegmentByIdPreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val tableEvent = event.asInstanceOf[DeleteSegmentByIdPreEvent]
    val carbonTable = tableEvent.carbonTable
    if (carbonTable != null) {
      if (CarbonTable.hasMVDataMap(carbonTable)) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on tables which have a mv datamap table")
      }
      val isMVdatamapTable = carbonTable.getTableInfo.getFactTable.getTableProperties
        .get("isMVdatamapTable")
      if (isMVdatamapTable != null && isMVdatamapTable.equals("true")) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on mv table")
      }
    }
  }
}

object MVDeleteSegmentByDatePreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val deleteSegmentByDatePreEvent = event.asInstanceOf[DeleteSegmentByDatePreEvent]
    val carbonTable = deleteSegmentByDatePreEvent.carbonTable
    if (carbonTable != null) {
      if (CarbonTable.hasMVDataMap(carbonTable)) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on tables which have a mv datamap table")
      }
      val isMVdatamapTable = carbonTable.getTableInfo.getFactTable.getTableProperties
        .get("isMVdatamapTable")
      if (isMVdatamapTable != null && isMVdatamapTable.equals("true")) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on mv table")
      }
    }
  }
}


