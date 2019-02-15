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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.exceptions.sql.NoSuchDataMapException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.log4j.Logger;

/**
 * Maintains the status of each datamap. As per the status query will decide whether to hit datamap
 * or not.
 */
public class DataMapStatusManager {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(DataMapStatusManager.class.getName());

  // Create private constructor to not allow create instance of it
  private DataMapStatusManager() {

  }

  /**
   * TODO Use factory when we have more storage providers
   */
  private static DataMapStatusStorageProvider storageProvider =
      new DiskBasedDataMapStatusProvider();

  /**
   * Reads all datamap status file
   * @return
   * @throws IOException
   */
  public static DataMapStatusDetail[] readDataMapStatusDetails() throws IOException {
    return storageProvider.getDataMapStatusDetails();
  }

  /**
   * Get enabled datamap status details
   * @return
   * @throws IOException
   */
  public static DataMapStatusDetail[] getEnabledDataMapStatusDetails() throws IOException {
    DataMapStatusDetail[] dataMapStatusDetails = storageProvider.getDataMapStatusDetails();
    List<DataMapStatusDetail> statusDetailList = new ArrayList<>();
    for (DataMapStatusDetail statusDetail : dataMapStatusDetails) {
      if (statusDetail.getStatus() == DataMapStatus.ENABLED) {
        statusDetailList.add(statusDetail);
      }
    }
    return statusDetailList.toArray(new DataMapStatusDetail[statusDetailList.size()]);
  }

  public static Map<String, DataMapStatusDetail> readDataMapStatusMap() throws IOException {
    DataMapStatusDetail[] details = storageProvider.getDataMapStatusDetails();
    Map<String, DataMapStatusDetail> map = new HashMap<>(details.length);
    for (DataMapStatusDetail detail : details) {
      map.put(detail.getDataMapName(), detail);
    }
    return map;
  }

  public static void disableDataMap(String dataMapName) throws IOException, NoSuchDataMapException {
    DataMapSchema dataMapSchema = getDataMapSchema(dataMapName);
    if (dataMapSchema != null) {
      List<DataMapSchema> list = new ArrayList<>();
      list.add(dataMapSchema);
      storageProvider.updateDataMapStatus(list, DataMapStatus.DISABLED);
    }
  }

  /**
   * This method will disable all lazy (DEFERRED REBUILD) datamap in the given table
   */
  public static void disableAllLazyDataMaps(CarbonTable table) throws IOException {
    List<DataMapSchema> allDataMapSchemas =
        DataMapStoreManager.getInstance().getDataMapSchemasOfTable(table);
    List<DataMapSchema> dataMapToBeDisabled = new ArrayList<>(allDataMapSchemas.size());
    for (DataMapSchema dataMap : allDataMapSchemas) {
      // TODO all non datamaps like MV is now supports only lazy. Once the support is made the
      // following check can be removed.
      if (dataMap.isLazy() || !dataMap.isIndexDataMap()) {
        dataMapToBeDisabled.add(dataMap);
      }
    }
    storageProvider.updateDataMapStatus(dataMapToBeDisabled, DataMapStatus.DISABLED);
  }

  public static void enableDataMap(String dataMapName) throws IOException, NoSuchDataMapException {
    DataMapSchema dataMapSchema = getDataMapSchema(dataMapName);
    if (dataMapSchema != null) {
      List<DataMapSchema> list = new ArrayList<>();
      list.add(dataMapSchema);
      storageProvider.updateDataMapStatus(list, DataMapStatus.ENABLED);
    }
  }

  public static void dropDataMap(String dataMapName) throws IOException, NoSuchDataMapException {
    DataMapSchema dataMapSchema = getDataMapSchema(dataMapName);
    if (dataMapSchema != null) {
      List<DataMapSchema> list = new ArrayList<>();
      list.add(dataMapSchema);
      storageProvider.updateDataMapStatus(list, DataMapStatus.DROPPED);
    }
  }

  private static DataMapSchema getDataMapSchema(String dataMapName)
      throws IOException, NoSuchDataMapException {
    return DataMapStoreManager.getInstance().getDataMapSchema(dataMapName);
  }

  /**
   * Returns valid segment list for a given RelationIdentifier
   *
   * @param relationIdentifier
   * @return
   * @throws IOException
   */
  public static List<String> getSegmentList(RelationIdentifier relationIdentifier)
      throws IOException {
    List<String> segmentList = new ArrayList<>();
    AbsoluteTableIdentifier absoluteTableIdentifier =
        AbsoluteTableIdentifier.from(relationIdentifier.getTablePath());
    List<Segment> validSegments =
        new SegmentStatusManager(absoluteTableIdentifier).getValidAndInvalidSegments()
            .getValidSegments();
    for (Segment segment : validSegments) {
      segmentList.add(segment.getSegmentNo());
    }
    return segmentList;
  }

  public static void cleanMVdatamap(CarbonTable carbonTable) throws IOException {
    List<DataMapSchema> allDataMapSchemas =
        DataMapStoreManager.getInstance().getDataMapSchemasOfTable(carbonTable);
    for (DataMapSchema datamapschema : allDataMapSchemas) {
      if (datamapschema.getProviderName()
          .equalsIgnoreCase(DataMapClassProvider.MV.getShortName())) {
        CarbonTable datamapTable = CarbonTable
            .buildFromTablePath(datamapschema.getRelationIdentifier().getTableName(),
                datamapschema.getRelationIdentifier().getDatabaseName(),
                datamapschema.getRelationIdentifier().getTablePath(),
                datamapschema.getRelationIdentifier().getTableId());
        SegmentStatusManager segmentStatusManager =
            new SegmentStatusManager(datamapTable.getAbsoluteTableIdentifier());
        ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();
        int retryCount = CarbonLockUtil
            .getLockProperty(CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK,
                CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK_DEFAULT);
        int maxTimeout = CarbonLockUtil
            .getLockProperty(CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK,
                CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK_DEFAULT);
        try {
          if (carbonLock.lockWithRetries(retryCount, maxTimeout)) {
            LOGGER.info(
                "Acquired lock for table" + datamapTable.getDatabaseName() + "." + datamapTable
                    .getTableName() + " for table status updation");
            LoadMetadataDetails[] loadMetadataDetails =
                SegmentStatusManager.readLoadMetadata(datamapTable.getMetadataPath());
            List<LoadMetadataDetails> listOfLoadFolderDetails =
                new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
            List<CarbonFile> staleFolders = new ArrayList<>();
            Collections.addAll(listOfLoadFolderDetails, loadMetadataDetails);
            for (LoadMetadataDetails entry : listOfLoadFolderDetails) {
              if (entry.getSegmentStatus() != SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS) {
                entry.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE);
                // For insert overwrite, we will delete the old segment folder immediately
                // So collect the old segments here
                String path = CarbonTablePath
                    .getSegmentPath(datamapschema.getRelationIdentifier().getTablePath(),
                        entry.getLoadName());
                if (FileFactory.isFileExist(path, FileFactory.getFileType(path))) {
                  staleFolders.add(FileFactory.getCarbonFile(path));
                }
              }
            }
            SegmentStatusManager.writeLoadDetailsIntoFile(CarbonTablePath
                    .getTableStatusFilePath(datamapschema.getRelationIdentifier().getTablePath()),
                listOfLoadFolderDetails
                    .toArray(new LoadMetadataDetails[listOfLoadFolderDetails.size()]));
            // Update sync information for mv
            storageProvider.setsyncinfo(datamapschema);
            // Delete all old stale segment folders
            for (CarbonFile staleFolder : staleFolders) {
              try {
                CarbonUtil.deleteFoldersAndFiles(staleFolder);
              } catch (IOException | InterruptedException e) {
                LOGGER.error("Failed to delete stale folder: " + e.getMessage(), e);
              }
            }
          }
        } finally {
          if (carbonLock.unlock()) {
            LOGGER.info("Table unlocked successfully after table status updation" + datamapTable
                .getDatabaseName() + "." + datamapTable.getTableName());
          } else {
            LOGGER.error(
                "Unable to unlock Table lock for table" + datamapTable.getDatabaseName() + "."
                    + datamapTable.getTableName() + " during table status updation");
          }
        }
      }
    }
  }

}
