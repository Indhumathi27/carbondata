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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;

import com.google.gson.Gson;
import org.apache.log4j.Logger;

/**
 * It saves/serializes the array of {{@link DataMapStatusDetail}} to disk in json format.
 * It ensures the data consistance while concurrent write through write lock. It saves the status
 * to the datamapstatus under the system folder.
 */
public class DiskBasedDataMapStatusProvider implements DataMapStatusStorageProvider {

  private static final Logger LOG =
      LogServiceFactory.getLogService(DiskBasedDataMapStatusProvider.class.getName());

  private static final String DATAMAP_STATUS_FILE = "datamapstatus";

  private static final String DATAMAP_SEGMENT_STATUS_FILE = "segmentstatus";

  @Override
  public DataMapStatusDetail[] getDataMapStatusDetails() throws IOException {
    String statusPath = CarbonProperties.getInstance().getSystemFolderLocation()
        + CarbonCommonConstants.FILE_SEPARATOR + DATAMAP_STATUS_FILE;
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    DataMapStatusDetail[] dataMapStatusDetails;
    try {
      if (!FileFactory.isFileExist(statusPath)) {
        return new DataMapStatusDetail[0];
      }
      dataInputStream =
          FileFactory.getDataInputStream(statusPath, FileFactory.getFileType(statusPath));
      inStream = new InputStreamReader(dataInputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      buffReader = new BufferedReader(inStream);
      dataMapStatusDetails = gsonObjectToRead.fromJson(buffReader, DataMapStatusDetail[].class);
    } catch (IOException e) {
      LOG.error("Failed to read datamap status", e);
      throw e;
    } finally {
      CarbonUtil.closeStreams(buffReader, inStream, dataInputStream);
    }

    // if dataMapStatusDetails is null, return empty array
    if (null == dataMapStatusDetails) {
      return new DataMapStatusDetail[0];
    }

    return dataMapStatusDetails;
  }

  /**
   * Update or add the status of passed datamaps with the given datamapstatus. If the datamapstatus
   * given is enabled/disabled then updates/adds the datamap, in case of drop it just removes it
   * from the file.
   * This method always overwrites the old file.
   * @param dataMapSchemas schemas of which are need to be updated in datamap status
   * @param dataMapStatus  status to be updated for the datamap schemas
   * @throws IOException
   */
  @Override
  public void updateDataMapStatus(List<DataMapSchema> dataMapSchemas, DataMapStatus dataMapStatus)
      throws IOException {
    if (dataMapSchemas == null || dataMapSchemas.size() == 0) {
      // There is nothing to update
      return;
    }
    ICarbonLock carbonTableStatusLock = getDataMapStatusLock();
    boolean locked = false;
    try {
      locked = carbonTableStatusLock.lockWithRetries();
      if (locked) {
        LOG.info("Datamap status lock has been successfully acquired.");
        DataMapStatusDetail[] dataMapStatusDetails = getDataMapStatusDetails();
        List<DataMapStatusDetail> dataMapStatusList = Arrays.asList(dataMapStatusDetails);
        dataMapStatusList = new ArrayList<>(dataMapStatusList);
        List<DataMapStatusDetail> changedStatusDetails = new ArrayList<>();
        List<DataMapStatusDetail> newStatusDetails = new ArrayList<>();
        for (DataMapSchema dataMapSchema : dataMapSchemas) {
          boolean exists = false;
          for (DataMapStatusDetail statusDetail : dataMapStatusList) {
            if (statusDetail.getDataMapName().equals(dataMapSchema.getDataMapName())) {
              statusDetail.setStatus(dataMapStatus);
              changedStatusDetails.add(statusDetail);
              exists = true;
            }
          }
          if (!exists) {
            newStatusDetails
                .add(new DataMapStatusDetail(dataMapSchema.getDataMapName(), dataMapStatus));
          }
        }
        // Add the newly added datamaps to the list.
        if (newStatusDetails.size() > 0 && dataMapStatus != DataMapStatus.DROPPED) {
          dataMapStatusList.addAll(newStatusDetails);
        }
        // In case of dropped datamap, just remove from the list.
        if (dataMapStatus == DataMapStatus.DROPPED) {
          dataMapStatusList.removeAll(changedStatusDetails);
        }
        writeLoadDetailsIntoFile(CarbonProperties.getInstance().getSystemFolderLocation()
                + CarbonCommonConstants.FILE_SEPARATOR + DATAMAP_STATUS_FILE,
            dataMapStatusList.toArray(new DataMapStatusDetail[dataMapStatusList.size()]));
      } else {
        String errorMsg = "Upadating datamapstatus is failed due to another process taken the lock"
            + " for updating it";
        LOG.error(errorMsg);
        throw new IOException(errorMsg + " Please try after some time.");
      }
    } finally {
      if (locked) {
        CarbonLockUtil.fileUnlock(carbonTableStatusLock, LockUsage.DATAMAP_STATUS_LOCK);
      }
    }
  }

  /**
   * writes datamap status details
   *
   * @param dataMapStatusDetails
   * @throws IOException
   */
  private static void writeLoadDetailsIntoFile(String location,
      DataMapStatusDetail[] dataMapStatusDetails) throws IOException {
    AtomicFileOperations fileWrite = AtomicFileOperationFactory.getAtomicFileOperations(location);
    BufferedWriter brWriter = null;
    DataOutputStream dataOutputStream = null;
    Gson gsonObjectToWrite = new Gson();
    // write the updated data into the datamap status file.
    try {
      dataOutputStream = fileWrite.openForWrite(FileWriteOperation.OVERWRITE);
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

      String metadataInstance = gsonObjectToWrite.toJson(dataMapStatusDetails);
      brWriter.write(metadataInstance);
    } catch (IOException ioe) {
      LOG.error("Error message: " + ioe.getLocalizedMessage());
      fileWrite.setFailed();
      throw ioe;
    } finally {
      if (null != brWriter) {
        brWriter.flush();
      }
      CarbonUtil.closeStreams(brWriter);
      fileWrite.close();
    }

  }

  private static ICarbonLock getDataMapStatusLock() {
    return CarbonLockFactory
        .getSystemLevelCarbonLockObj(CarbonProperties.getInstance().getSystemFolderLocation(),
            LockUsage.DATAMAP_STATUS_LOCK);
  }

  /**
   * Reads and returns dataMapSegmentStatusDetail
   *
   * @param dataMapSchema
   * @throws IOException
   */
  public DataMapSegmentStatusDetail getDataMapSegmentStatus(DataMapSchema dataMapSchema)
      throws IOException {
    String statusPath = getDatamapSegmentStatusFile(dataMapSchema.getDataMapName());
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    DataMapSegmentStatusDetail dataMapSegmentStatusDetail;
    try {
      if (!FileFactory.isFileExist(statusPath)) {
        return new DataMapSegmentStatusDetail();
      }
      dataInputStream =
          FileFactory.getDataInputStream(statusPath, FileFactory.getFileType(statusPath));
      inStream = new InputStreamReader(dataInputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      buffReader = new BufferedReader(inStream);
      dataMapSegmentStatusDetail =
          gsonObjectToRead.fromJson(buffReader, DataMapSegmentStatusDetail.class);
    } catch (IOException e) {
      LOG.error("Failed to read datamap segment status", e);
      throw e;
    } finally {
      CarbonUtil.closeStreams(buffReader, inStream, dataInputStream);
    }

    if (null == dataMapSegmentStatusDetail) {
      return new DataMapSegmentStatusDetail();
    }
    return dataMapSegmentStatusDetail;
  }

  /**
   * After each data load to mv datamap, update the segment status mapping. Get the new load name
   * from datamap table loadMetaDetails and map newly loaded main table segments against the datamap
   * table new load entry
   *
   * @param dataMapSchema
   * @throws IOException
   */
  public void updateSegmentMapping(DataMapSchema dataMapSchema) throws IOException {
    DataMapSegmentStatusDetail dataMapSegmentStatus = getDataMapSegmentStatus(dataMapSchema);
    List<RelationIdentifier> relationIdentifiers = dataMapSchema.getParentTables();
    CarbonTable dataMapTable = CarbonTable
        .buildFromTablePath(dataMapSchema.getRelationIdentifier().getTableName(),
            dataMapSchema.getRelationIdentifier().getDatabaseName(),
            dataMapSchema.getRelationIdentifier().getTablePath(),
            dataMapSchema.getRelationIdentifier().getTableId());
    LoadMetadataDetails[] loadMetadataDetails =
        SegmentStatusManager.readLoadMetadata(dataMapTable.getMetadataPath());
    if (loadMetadataDetails.length != 0) {
      String newLoadKey;
      if (!dataMapSegmentStatus.getSegmentMapping().isEmpty()) {
        for (LoadMetadataDetails entry : loadMetadataDetails) {
          if (entry.getSegmentStatus() == SegmentStatus.MARKED_FOR_DELETE
              || entry.getSegmentStatus() == SegmentStatus.COMPACTED) {
            dataMapSegmentStatus.getSegmentMapping().remove(entry.getLoadName());
          }
        }
      } else {
        dataMapSegmentStatus.setDataMapName(dataMapSchema.getDataMapName());
      }
      newLoadKey = loadMetadataDetails[loadMetadataDetails.length - 1].getLoadName();
      Map<String, List<String>> mainTableSegmentMap = new HashMap<>();
      for (RelationIdentifier relationIdentifier : relationIdentifiers) {
        List<String> validSegmentList = DataMapStatusManager.getSegmentList(relationIdentifier);
        List<String> datamapTableSegmentList =
            getDataMapSegmentsFromMapping(dataMapSegmentStatus, relationIdentifier);
        validSegmentList.removeAll(datamapTableSegmentList);
        mainTableSegmentMap.put(relationIdentifier.getTableName(), validSegmentList);
      }
      dataMapSegmentStatus.getSegmentMapping().put(newLoadKey, mainTableSegmentMap);
      dataMapSegmentStatus.setSegmentMapping(dataMapSegmentStatus.getSegmentMapping());
      writeToSegmentStatusFile(dataMapSegmentStatus, dataMapSchema.getDataMapName());
    }
  }

  /**
   * write datamap to mainTbale segment mapping details
   *
   * @param dataMapSegmentStatus
   * @param dataMapName
   * @throws IOException
   */
  private void writeToSegmentStatusFile(DataMapSegmentStatusDetail dataMapSegmentStatus,
      String dataMapName) throws IOException {
    ICarbonLock carbonTableStatusLock = getDataMapStatusLock();
    try {
      if (carbonTableStatusLock.lockWithRetries()) {
        writeSegmentDetailsIntoFile(getDatamapSegmentStatusFile(dataMapName), dataMapSegmentStatus);
      } else {
        String errorMsg = "Not able to acquire the lock for DataMap Segment status updation";
        LOG.error(errorMsg);
        throw new IOException(errorMsg);
      }
    } finally {
      if (carbonTableStatusLock.unlock()) {
        CarbonLockUtil.fileUnlock(carbonTableStatusLock, LockUsage.DATAMAP_STATUS_LOCK);
      }
    }
  }

  /**
   * Returns list of segments of mainTable which are already loaded to MV dataMap table
   *
   * @param dataMapSegmentStatus
   * @param relationIdentifier
   */
  public List<String> getDataMapSegmentsFromMapping(DataMapSegmentStatusDetail dataMapSegmentStatus,
      RelationIdentifier relationIdentifier) {
    List<String> dataMapTableSegmentList = new ArrayList<>();
    for (Map.Entry<String, Map<String, List<String>>> dataMapSegmentIterator : dataMapSegmentStatus
        .getSegmentMapping().entrySet()) {
      for (Map.Entry<String, List<String>> mainTableSegmentIterator : dataMapSegmentIterator
          .getValue().entrySet()) {
        String mainTableName = mainTableSegmentIterator.getKey();
        if (mainTableName.equalsIgnoreCase(relationIdentifier.getTableName())) {
          dataMapTableSegmentList.addAll(mainTableSegmentIterator.getValue());
        }
      }
    }
    return dataMapTableSegmentList;
  }

  /**
   * Update datamap segment status mapping if datamap table is compacted
   *
   * @param dataMapSchema
   * @param loadMetadataDetails
   * @throws IOException
   */
  public void updateMappingAfterCompaction(DataMapSchema dataMapSchema,
      LoadMetadataDetails[] loadMetadataDetails) throws IOException {
    DataMapSegmentStatusDetail dataMapSegmentStatus = getDataMapSegmentStatus(dataMapSchema);
    List<RelationIdentifier> relationIdentifiers = dataMapSchema.getParentTables();
    Set<String> dataMapSegmentStatusList = dataMapSegmentStatus.getSegmentMapping().keySet();
    Set<String> dataMapSegmentListAfterCom = new HashSet<>();
    // Get all valid segments of datamap table after compaction
    for (LoadMetadataDetails loadMetadataDetail : loadMetadataDetails) {
      if (loadMetadataDetail.getSegmentStatus() == SegmentStatus.SUCCESS) {
        dataMapSegmentListAfterCom.add(loadMetadataDetail.getLoadName());
      }
    }
    // Get valid segments list which are not present in dataMapSegmentStatusList
    dataMapSegmentListAfterCom.removeAll(dataMapSegmentStatusList);
    LoadMetadataDetails[] loadMetadataDetailsAfterCom =
        new LoadMetadataDetails[dataMapSegmentListAfterCom.size()];
    int i = 0;
    // Get list of loadMetaDetails for compacted segments
    for (String compactedSegment : dataMapSegmentListAfterCom) {
      for (LoadMetadataDetails loadMetadataDetail : loadMetadataDetails) {
        if (compactedSegment.equalsIgnoreCase(loadMetadataDetail.getLoadName())) {
          loadMetadataDetailsAfterCom[i] = loadMetadataDetail;
          i++;
        }
      }
    }
    Map<String, Map<String, List<String>>> newSegmentStatusMap = new HashMap<>();
    // Iterate over the segment list to find which all segments are merged to compacted segment and
    // remove those segments from datamap table segment list
    for (LoadMetadataDetails compactedloadMetadataDetail : loadMetadataDetailsAfterCom) {
      Map<String, List<String>> mainTableMapping = new HashMap<>();
      List<String> datamapSegmentsToBeRemoved = new ArrayList<>();
      for (LoadMetadataDetails loadMetadataDetail : loadMetadataDetails) {
        if (compactedloadMetadataDetail.getLoadName()
            .equalsIgnoreCase(loadMetadataDetail.getMergedLoadName())) {
          datamapSegmentsToBeRemoved.add(loadMetadataDetail.getLoadName());
        }
      }
      for (RelationIdentifier relationIdentifier : relationIdentifiers) {
        List<String> segmentList = new ArrayList<>();
        for (String list : datamapSegmentsToBeRemoved) {
          Map<String, List<String>> dataMapSegmentMap =
              dataMapSegmentStatus.getSegmentMapping().get(list);
          for (Map.Entry<String, List<String>> mainTableMap : dataMapSegmentMap.entrySet()) {
            if (mainTableMap.getKey().equalsIgnoreCase(relationIdentifier.getTableName())) {
              segmentList.addAll(dataMapSegmentMap.get(relationIdentifier.getTableName()));
            }
          }
        }
        mainTableMapping.put(relationIdentifier.getTableName(), segmentList);
      }
      for (String entryTobeRemoved : datamapSegmentsToBeRemoved) {
        dataMapSegmentStatus.getSegmentMapping().remove(entryTobeRemoved);
      }
      newSegmentStatusMap.put(compactedloadMetadataDetail.getLoadName(), mainTableMapping);
    }
    dataMapSegmentStatus.getSegmentMapping().putAll(newSegmentStatusMap);
    writeSegmentDetailsIntoFile(getDatamapSegmentStatusFile(dataMapSchema.getDataMapName()),
        dataMapSegmentStatus);
  }

  /**
   * writes datamap segement status details
   *
   * @param dataMapSegmentStatusDetail
   * @throws IOException
   */
  private static void writeSegmentDetailsIntoFile(String location,
      DataMapSegmentStatusDetail dataMapSegmentStatusDetail) throws IOException {
    AtomicFileOperations fileWrite = AtomicFileOperationFactory.getAtomicFileOperations(location);
    BufferedWriter brWriter = null;
    DataOutputStream dataOutputStream = null;
    Gson gsonObjectToWrite = new Gson();
    // write the updated data into the datamap segment status file.
    try {
      dataOutputStream = fileWrite.openForWrite(FileWriteOperation.OVERWRITE);
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

      String metadataInstance = gsonObjectToWrite.toJson(dataMapSegmentStatusDetail);
      brWriter.write(metadataInstance);
    } catch (IOException ioe) {
      LOG.error("Error message: " + ioe.getLocalizedMessage());
      fileWrite.setFailed();
      throw ioe;
    } finally {
      if (null != brWriter) {
        brWriter.flush();
      }
      CarbonUtil.closeStreams(brWriter);
      fileWrite.close();
    }
  }

  private static String getDatamapSegmentStatusFile(String datamapName) {
    return CarbonProperties.getInstance().getSystemFolderLocation()
        + CarbonCommonConstants.FILE_SEPARATOR + datamapName + "." + DATAMAP_SEGMENT_STATUS_FILE;
  }

  /**
   * Clears datamap segment status map and delete datamapSegmentStatus file when MV
   * datamap is dropped
   *
   * @param dataMapSchema
   * @throws IOException
   */
  public void deleteSegmentStatusFile(DataMapSchema dataMapSchema) throws IOException {
    String segmentStatusPath = getDatamapSegmentStatusFile(dataMapSchema.getDataMapName());
    if (!FileFactory.isFileExist(segmentStatusPath, FileFactory.getFileType(segmentStatusPath))) {
      throw new IOException("DataMap Status file for datamap  " + dataMapSchema.getDataMapName()
          + " does not exists in storage");
    }
    DataMapSegmentStatusDetail dataMapSegmentStatus = getDataMapSegmentStatus(dataMapSchema);
    dataMapSegmentStatus.clear();
    if (!FileFactory.deleteFile(segmentStatusPath, FileFactory.getFileType(segmentStatusPath))) {
      throw new IOException("DataMap Status file for datamap  " + dataMapSchema.getDataMapName()
          + " cannot be deleted");
    }
  }

  /**
   * Remove all entries from datamapSegmentStatus file in case of Insert-OverWrite/Update Operation
   * on main table
   *
   * @param dataMapSchema
   * @throws IOException
   */
  public void clearSegmentMapping(DataMapSchema dataMapSchema) throws IOException {
    DataMapSegmentStatusDetail dataMapSegmentStatus = getDataMapSegmentStatus(dataMapSchema);
    Map<String, Map<String, List<String>>> segmentMap = dataMapSegmentStatus.getSegmentMapping();
    segmentMap.clear();
    dataMapSegmentStatus.setSegmentMapping(segmentMap);
    writeToSegmentStatusFile(dataMapSegmentStatus, dataMapSchema.getDataMapName());
  }

}
