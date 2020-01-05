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

package org.apache.carbondata.core.metadata.schema.table;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.common.exceptions.sql.NoSuchDataMapException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;

/**
 * Stores datamap schema in disk as json format
 */
public class DiskBasedDMSchemaStorageProvider implements DataMapSchemaStorageProvider {

  private Logger LOG = LogServiceFactory.getLogService(this.getClass().getCanonicalName());

  private String storePath;

  private String mdtFilePath;

  private long lastModifiedTime;

  private Set<DataMapSchema> dataMapSchemas = new HashSet<>();

  private Set<String> mdtFiles = new HashSet<>();

  private Set<String> storePathList = new HashSet<>();

  public DiskBasedDMSchemaStorageProvider(String storePath) {
    this.storePath = CarbonUtil.checkAndAppendHDFSUrl(storePath);
    this.mdtFilePath = storePath + CarbonCommonConstants.FILE_SEPARATOR + "datamap.mdtfile";
    initFiles();
  }

  public void initFiles() {
    String sysFolder = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_SYSTEM_FOLDER_LOCATION_ACROSS_DATABASE);
    String[] folders = sysFolder.split(",");
    for (String storePathDB : folders) {
      String newPath = FileFactory.getUpdatedFilePath(CarbonUtil.checkAndAppendHDFSUrl(storePathDB))
          + CarbonCommonConstants.FILE_SEPARATOR + "_system";
      this.storePathList.add(newPath);
      this.mdtFiles.add((newPath + CarbonCommonConstants.FILE_SEPARATOR + "datamap.mdtfile"));
    }
  }

  @Override
  public void saveSchema(DataMapSchema dataMapSchema) throws IOException {
    BufferedWriter brWriter = null;
    DataOutputStream dataOutputStream = null;
    Gson gsonObjectToWrite = new Gson();
    String schemaPath = getSchemaPath(storePath, dataMapSchema.getDataMapName());
    if (FileFactory.isFileExist(schemaPath)) {
      throw new IOException(
          "DataMap with name " + dataMapSchema.getDataMapName() + " already exists in storage");
    }
    // write the datamap shema in json format.
    try {
      FileFactory.mkdirs(storePath);
      FileFactory.createNewFile(schemaPath);
      dataOutputStream =
          FileFactory.getDataOutputStream(schemaPath);
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

      String metadataInstance = gsonObjectToWrite.toJson(dataMapSchema);
      brWriter.write(metadataInstance);
    } finally {
      if (null != brWriter) {
        brWriter.flush();
      }
      dataMapSchemas.add(dataMapSchema);
      CarbonUtil.closeStreams(dataOutputStream, brWriter);
      checkAndReloadDataMapSchemas(true);
      touchMDTFile();
    }
  }

  @Override
  public DataMapSchema retrieveSchema(String dataMapName, String databaseName)
      throws IOException, NoSuchDataMapException {
    checkAndReloadDataMapSchemas(true);
    for (DataMapSchema dataMapSchema : dataMapSchemas) {
      if (dataMapSchema.getDataMapName().equalsIgnoreCase(dataMapName) && dataMapSchema
          .getRelationIdentifier().getDatabaseName().equalsIgnoreCase(databaseName)) {
        return dataMapSchema;
      }
    }
    throw new NoSuchDataMapException(dataMapName);
  }

  @Override
  public List<DataMapSchema> retrieveSchemas(CarbonTable carbonTable) throws IOException {
    checkAndReloadDataMapSchemas(true);
    List<DataMapSchema> dataMapSchemas = new ArrayList<>();
    for (DataMapSchema dataMapSchema : this.dataMapSchemas) {
      List<RelationIdentifier> parentTables = dataMapSchema.getParentTables();
      for (RelationIdentifier identifier : parentTables) {
        if (StringUtils.isNotEmpty(identifier.getTableId())) {
          if (identifier.getTableId().equalsIgnoreCase(carbonTable.getTableId())) {
            dataMapSchemas.add(dataMapSchema);
            break;
          }
        } else if (identifier.getTableName().equalsIgnoreCase(carbonTable.getTableName()) &&
            identifier.getDatabaseName().equalsIgnoreCase(carbonTable.getDatabaseName())) {
          dataMapSchemas.add(dataMapSchema);
          break;
        }
      }
    }
    return dataMapSchemas;
  }

  @Override
  public List<DataMapSchema> retrieveAllSchemas() throws IOException {
    checkAndReloadDataMapSchemas(true);
    return new ArrayList<>(dataMapSchemas);
  }

  private Set<DataMapSchema> retrieveAllSchemasInternal() throws IOException {
    Set<DataMapSchema> dataMapSchemas = new HashSet<>();
    for (String storePath : storePathList) {
      CarbonFile carbonFile = FileFactory.getCarbonFile(storePath);
      CarbonFile[] carbonFiles = carbonFile.listFiles(new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile file) {
          return file.getName().endsWith(".dmschema");
        }
      });

      for (CarbonFile file : carbonFiles) {
        Gson gsonObjectToRead = new Gson();
        DataInputStream dataInputStream = null;
        BufferedReader buffReader = null;
        InputStreamReader inStream = null;
        try {
          String absolutePath = file.getAbsolutePath();
          dataInputStream = FileFactory.getDataInputStream(absolutePath);
          inStream = new InputStreamReader(dataInputStream,
              Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
          buffReader = new BufferedReader(inStream);
          dataMapSchemas.add(gsonObjectToRead.fromJson(buffReader, DataMapSchema.class));
        } finally {
          CarbonUtil.closeStreams(buffReader, inStream, dataInputStream);
        }
      }
    }
    return dataMapSchemas;
  }

  @Override
  public void dropSchema(String dataMapName)
      throws IOException {
    String schemaPath = getSchemaPath(storePath, dataMapName);
    if (!FileFactory.isFileExist(schemaPath)) {
      throw new IOException("DataMap with name " + dataMapName + " does not exists in storage");
    }

    LOG.info(String.format("Trying to delete DataMap %s schema", dataMapName));

    dataMapSchemas.removeIf(schema -> schema.getDataMapName().equalsIgnoreCase(dataMapName));
    touchMDTFile();
    if (!FileFactory.deleteFile(schemaPath)) {
      throw new IOException("DataMap with name " + dataMapName + " cannot be deleted");
    }
    LOG.info(String.format("DataMap %s schema is deleted", dataMapName));
  }

  private void checkAndReloadDataMapSchemas(boolean touchFile) throws IOException {
    if (FileFactory.isFileExist(mdtFilePath)) {
      long lastModifiedTime = FileFactory.getCarbonFile(mdtFilePath).getLastModifiedTime();
      if (this.lastModifiedTime != lastModifiedTime) {
        dataMapSchemas = retrieveAllSchemasInternal();
        touchMDTFile();
      }
    } else {
      dataMapSchemas = retrieveAllSchemasInternal();
      if (touchFile) {
        touchMDTFile();
      }
    }
  }

  private void touchMDTFile() throws IOException {
    if (!FileFactory.isFileExist(storePath)) {
      FileFactory.createDirectoryAndSetPermission(
          storePath,
          new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    }
    if (!FileFactory.isFileExist(mdtFilePath)) {
      FileFactory.createNewFile(
          mdtFilePath,
          new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    }
    long lastModifiedTime = System.currentTimeMillis();
    FileFactory.getCarbonFile(mdtFilePath).setLastModifiedTime(lastModifiedTime);
    this.lastModifiedTime = lastModifiedTime;
  }

  /**
   * it returns the schema path for the datamap
   * @param storePath
   * @param dataMapName
   * @return
   */
  public static String getSchemaPath(String storePath, String dataMapName) {
    String schemaPath =  storePath + CarbonCommonConstants.FILE_SEPARATOR + dataMapName
        + ".dmschema";
    return schemaPath;
  }

  public static void main(String[] args) {
    DataMapSchema dm1 = new DataMapSchema("dm1", "mv");
    DataMapSchema dm2 = new DataMapSchema("dm1","mv");
    Set<DataMapSchema> se = new HashSet<>();
    se.add(dm1);
    se.add(dm2);
    System.out.println(se.size());
  }
}
