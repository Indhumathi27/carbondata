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
package org.apache.carbondata.core.datamap.status;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;

/**
 * Status of each datamap
 */
@InterfaceAudience.Internal
public class DataMapStatusDetail implements Serializable {

  private static final long serialVersionUID = 1570997199499681821L;
  private String dataMapName;

  private DataMapStatus status;

  /**
   * syncInfo is the map containing the datamap's main table as key and corresponding list of
   * segment ids as values till which the datamap is synced with main table data
   */
  private Map<String, List<String>> syncInfo;

  public DataMapStatusDetail() {
  }

  public DataMapStatusDetail(String dataMapName, DataMapStatus status,
      Map<String, List<String>> syncInfo) {
    this.dataMapName = dataMapName;
    this.status = status;
    this.syncInfo = syncInfo;
  }

  public String getDataMapName() {
    return dataMapName;
  }

  public void setDataMapName(String dataMapName) {
    this.dataMapName = dataMapName;
  }

  public DataMapStatus getStatus() {
    return status;
  }

  public boolean isEnabled() {
    return status == DataMapStatus.ENABLED;
  }

  public void setStatus(DataMapStatus status) {
    this.status = status;
  }

  public Map<String, List<String>> getSegmentInfo() {
    return syncInfo;
  }

  public void setSyncInfo(Map<String, List<String>> syncInfo) {
    this.syncInfo = syncInfo;
  }
}
