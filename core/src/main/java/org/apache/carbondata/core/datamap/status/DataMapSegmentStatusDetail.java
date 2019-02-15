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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;

/**
 * Segment Mapping Status for an datamap
 */
@InterfaceAudience.Internal
public class DataMapSegmentStatusDetail implements Serializable {

  private String dataMapName;

  private Map<String, Map<String, List<String>>> segmentMapping = new HashMap<>();

  public DataMapSegmentStatusDetail(String dataMapName,
      Map<String, Map<String, List<String>>> segmentMapping) {
    this.dataMapName = dataMapName;
    this.segmentMapping = segmentMapping;
  }

  public DataMapSegmentStatusDetail() {
  }

  public void setDataMapName(String dataMapName) {
    this.dataMapName = dataMapName;
  }

  public String getDataMapName() {
    return dataMapName;
  }

  public void setSegmentMapping(Map<String, Map<String, List<String>>> segmentMapping) {
    this.segmentMapping = segmentMapping;
  }

  public Map<String, Map<String, List<String>>> getSegmentMapping() {
    return segmentMapping;
  }

  public void clear() {
    segmentMapping.clear();
  }
}
