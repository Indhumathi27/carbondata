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

package org.apache.carbondata.core.scan.result;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.ReusableDataBuffer;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.mutate.DeleteDeltaVo;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.scan.scanner.LazyBlockletLoader;
import org.apache.carbondata.core.scan.scanner.LazyPageLoader;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsModel;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.ReUsableByteArrayDataOutputStream;

import org.apache.log4j.Logger;

/**
 * Scanned result class which will store and provide the result on request
 */
public abstract class BlockletScannedResult {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(BlockletScannedResult.class.getName());
  /**
   * current row number
   */
  protected int currentRow = -1;

  protected int pageCounter;
  /**
   * matched rowId for each page
   */
  protected int[][] pageFilteredRowId;
  /**
   * key size of the fixed length column
   */
  protected int fixedLengthKeySize;
  /**
   * total number of filtered rows for each page
   */
  private int[] pageFilteredRowCount;

  /**
   * Filtered pages to be decoded and loaded to vector.
   */
  private int[] pageIdFiltered;

  /**
   * to keep track of number of rows process
   */
  protected int rowCounter;
  /**
   * dimension column data chunk
   */
  protected DimensionColumnPage[][] dimensionColumnPages;

  /**
   * Raw dimension chunks;
   */
  protected DimensionRawColumnChunk[] dimRawColumnChunks;

  /**
   * Raw dimension chunks;
   */
  protected MeasureRawColumnChunk[] msrRawColumnChunks;
  /**
   * measure column data chunk
   */
  protected ColumnPage[][] measureColumnPages;
  /**
   * dictionary column block index in file
   */
  protected int[] dictionaryColumnChunkIndexes;

  /**
   * no dictionary column chunk index in file
   */
  protected int[] noDictionaryColumnChunkIndexes;

  /**
   *
   */
  public Map<Integer, GenericQueryType> complexParentIndexToQueryMap;

  private int totalDimensionsSize;

  /**
   * blockedId which will be blockId + blocklet number in the block
   */
  private String blockletId;

  /**
   * parent block indexes
   */
  private int[] complexParentBlockIndexes;

  /**
   * blockletId+pageNumber to deleted record map
   */
  private Map<String, DeleteDeltaVo> deletedRecordMap;

  /**
   * current page delete delta vo
   */
  private DeleteDeltaVo currentDeleteDeltaVo;

  /**
   * actual blocklet number
   */
  private String blockletNumber;

  protected List<Integer> validRowIds;

  protected QueryStatisticsModel queryStatisticsModel;

  protected LazyBlockletLoader lazyBlockletLoader;

  private ReusableDataBuffer[] dimensionReusableBuffer;

  private ReusableDataBuffer[] measureReusableBuffer;

  // index used by dimensionReusableBuffer
  private int dimensionReusableBufferIndex;

  public BlockletScannedResult(BlockExecutionInfo blockExecutionInfo,
      QueryStatisticsModel queryStatisticsModel) {
    this.dimensionReusableBuffer = blockExecutionInfo.getDimensionReusableDataBuffer();
    this.measureReusableBuffer = blockExecutionInfo.getMeasureReusableDataBuffer();
    this.fixedLengthKeySize = blockExecutionInfo.getFixedLengthKeySize();
    this.noDictionaryColumnChunkIndexes = blockExecutionInfo.getNoDictionaryColumnChunkIndexes();
    this.dictionaryColumnChunkIndexes = blockExecutionInfo.getDictionaryColumnChunkIndex();
    this.complexParentIndexToQueryMap = blockExecutionInfo.getComplexDimensionInfoMap();
    this.complexParentBlockIndexes = blockExecutionInfo.getComplexColumnParentBlockIndexes();
    this.totalDimensionsSize = blockExecutionInfo.getProjectionDimensions().length;
    this.deletedRecordMap = blockExecutionInfo.getDeletedRecordsMap();
    this.queryStatisticsModel = queryStatisticsModel;
    validRowIds = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  }

  /**
   * Below method will be used to set the dimension chunks
   * which will be used to create a row
   *
   * @param columnPages dimension chunks used in query
   */
  public void setDimensionColumnPages(DimensionColumnPage[][] columnPages) {
    this.dimensionColumnPages = columnPages;
  }

  /**
   * Below method will be used to set the measure column chunks
   *
   * @param columnPages measure data chunks
   */
  public void setMeasureColumnPages(ColumnPage[][] columnPages) {
    this.measureColumnPages = columnPages;
  }

  public void setDimRawColumnChunks(DimensionRawColumnChunk[] dimRawColumnChunks) {
    this.dimRawColumnChunks = dimRawColumnChunks;
  }

  public void setMsrRawColumnChunks(MeasureRawColumnChunk[] msrRawColumnChunks) {
    this.msrRawColumnChunks = msrRawColumnChunks;
  }

  public LazyBlockletLoader getLazyBlockletLoader() {
    return lazyBlockletLoader;
  }

  public void setLazyBlockletLoader(LazyBlockletLoader lazyBlockletLoader) {
    this.lazyBlockletLoader = lazyBlockletLoader;
  }

  /**
   * Below method will be used to get the chunk based in measure ordinal
   *
   * @param ordinal measure ordinal
   * @return measure column chunk
   */
  public ColumnPage getMeasureChunk(int ordinal) {
    return measureColumnPages[ordinal][pageCounter];
  }

  /**
   * Below method will be used to get the key for all the dictionary dimensions
   * in integer array format which is present in the query
   *
   * @param rowId row id selected after scanning
   * @return return the dictionary key
   */
  protected int[] getDictionaryKeyIntegerArray(int rowId) {
    int[] completeKey = new int[totalDimensionsSize];
    int column = 0;
    for (int i = 0; i < this.dictionaryColumnChunkIndexes.length; i++) {
      column = dimensionColumnPages[dictionaryColumnChunkIndexes[i]][pageCounter]
          .fillSurrogateKey(rowId, column, completeKey);
    }
    return completeKey;
  }

  /**
   * Fill the column data of dictionary to vector
   */
  public void fillColumnarDictionaryBatch(ColumnVectorInfo[] vectorInfo) {
    int column = 0;
    for (int i = 0; i < this.dictionaryColumnChunkIndexes.length; i++) {
      column = dimensionColumnPages[dictionaryColumnChunkIndexes[i]][pageCounter]
          .fillVector(vectorInfo, column);
    }
  }

  /**
   * Fill the column data to vector
   */
  public void fillColumnarNoDictionaryBatch(ColumnVectorInfo[] vectorInfo) {
    for (int i = 0; i < this.noDictionaryColumnChunkIndexes.length; i++) {
      dimensionColumnPages[noDictionaryColumnChunkIndexes[i]][pageCounter]
          .fillVector(vectorInfo, i);
    }
  }

  /**
   * Fill the measure column data to vector
   */
  public void fillColumnarMeasureBatch(ColumnVectorInfo[] vectorInfo, int[] measuresOrdinal) {
    for (int i = 0; i < measuresOrdinal.length; i++) {
      vectorInfo[i].measureVectorFiller
          .fillMeasureVector(measureColumnPages[measuresOrdinal[i]][pageCounter], vectorInfo[i]);
    }
  }

  public void fillColumnarComplexBatch(ColumnVectorInfo[] vectorInfos) {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    ReUsableByteArrayDataOutputStream reusableDataOutput =
        new ReUsableByteArrayDataOutputStream(byteStream);
    boolean isExceptionThrown = false;
    for (ColumnVectorInfo vectorInfo : vectorInfos) {
      int offset = vectorInfo.offset;
      int len = offset + vectorInfo.size;
      int vectorOffset = vectorInfo.vectorOffset;
      CarbonColumnVector vector = vectorInfo.vector;
      for (int j = offset; j < len; j++) {
        try {
          vectorInfo.genericQueryType
              .parseBlocksAndReturnComplexColumnByteArray(dimRawColumnChunks, dimensionColumnPages,
                  pageFilteredRowId == null ? j : pageFilteredRowId[pageCounter][j], pageCounter,
                  reusableDataOutput);
          Object data = vectorInfo.genericQueryType
              .getDataBasedOnDataType(ByteBuffer.wrap(reusableDataOutput.getByteArray()));
          vector.putObject(vectorOffset++, data);
          reusableDataOutput.reset();
        } catch (IOException e) {
          isExceptionThrown = true;
          LOGGER.error(e.getMessage(), e);
        } finally {
          if (isExceptionThrown) {
            CarbonUtil.closeStreams(reusableDataOutput);
            CarbonUtil.closeStreams(byteStream);
          }
        }
      }
    }
    CarbonUtil.closeStreams(reusableDataOutput);
    CarbonUtil.closeStreams(byteStream);
  }

  /**
   * Fill the column data to vector
   */
  public void fillColumnarImplicitBatch(ColumnVectorInfo[] vectorInfo) {
    for (int i = 0; i < vectorInfo.length; i++) {
      ColumnVectorInfo columnVectorInfo = vectorInfo[i];
      CarbonColumnVector vector = columnVectorInfo.vector;
      int offset = columnVectorInfo.offset;
      int vectorOffset = columnVectorInfo.vectorOffset;
      int len = offset + columnVectorInfo.size;
      for (int j = offset; j < len; j++) {
        // Considering only String case now as we support only
        String data = getBlockletId();
        if (CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID
            .equals(columnVectorInfo.dimension.getColumnName())) {
          data = data + CarbonCommonConstants.FILE_SEPARATOR + pageCounter
              + CarbonCommonConstants.FILE_SEPARATOR + (pageFilteredRowId == null ? j :
              pageFilteredRowId[pageCounter][j]);
        }
        vector.putByteArray(vectorOffset++,
            data.getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));
      }
    }
  }

  /**
   * Just increment the counter in case of query only on measures.
   */
  public void incrementCounter() {
    rowCounter++;
    currentRow++;
  }

  /**
   * This method will add the delta to row counter
   *
   * @param delta
   */
  public void incrementCounter(int delta) {
    rowCounter += delta;
    currentRow += delta;
  }

  /**
   * Just increment the page counter and reset the remaining counters.
   */
  public void incrementPageCounter() {
    rowCounter = 0;
    currentRow = -1;
    pageCounter++;
    fillDataChunks();
    if (null != deletedRecordMap) {
      currentDeleteDeltaVo = deletedRecordMap.get(blockletNumber + "_" + pageCounter);
    }
  }

  /**
   * Just increment the page counter and reset the remaining counters.
   */
  public void incrementPageCounter(ColumnVectorInfo[] vectorInfos) {
    rowCounter = 0;
    currentRow = -1;
    pageCounter++;
    if (null != deletedRecordMap && pageCounter < pageIdFiltered.length) {
      currentDeleteDeltaVo =
          deletedRecordMap.get(blockletNumber + "_" + pageIdFiltered[pageCounter]);
    }
  }

  /**
   * This case is used only in case of compaction, since it does not use filter flow.
   */
  public void fillDataChunks() {
    freeDataChunkMemory();
    if (pageCounter >= pageFilteredRowCount.length) {
      return;
    }
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < dimensionColumnPages.length; i++) {
      if (dimensionColumnPages[i][pageCounter] == null && dimRawColumnChunks[i] != null) {
        dimensionColumnPages[i][pageCounter] = dimRawColumnChunks[i]
            .convertToDimColDataChunkWithOutCache(pageCounter, null);
      }
    }

    for (int i = 0; i < measureColumnPages.length; i++) {
      if (measureColumnPages[i][pageCounter] == null && msrRawColumnChunks[i] != null) {
        measureColumnPages[i][pageCounter] = msrRawColumnChunks[i]
            .convertToColumnPageWithOutCache(pageCounter, null);
      }
    }
    QueryStatistic pageUncompressTime = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.PAGE_UNCOMPRESS_TIME);
    pageUncompressTime.addCountStatistic(QueryStatisticsConstants.PAGE_UNCOMPRESS_TIME,
        pageUncompressTime.getCount() + (System.currentTimeMillis() - startTime));
  }

  void fillChildDataChunks(CarbonColumnVector columnVector, CarbonDimension dimension,
      ColumnVectorInfo complexInfo) {
    columnVector.setLazyPage(new LazyPageLoader(lazyBlockletLoader, dimension.getOrdinal(), false,
        pageIdFiltered[pageCounter], complexInfo,
        dimensionReusableBuffer[dimensionReusableBufferIndex++]));
    if (columnVector.getChildrenVector() != null) {
      int i = 0;
      for (CarbonColumnVector childVector : columnVector.getChildrenVector()) {
        fillChildDataChunks(childVector, dimension.getListOfChildDimensions().get(i++),
            complexInfo);
      }
    }
  }

  /**
   * Fill all the vectors with data by decompressing/decoding the column page
   */
  public void fillDataChunks(ColumnVectorInfo[] dictionaryInfo, ColumnVectorInfo[] noDictionaryInfo,
      ColumnVectorInfo[] msrVectorInfo, int[] measuresOrdinal, ColumnVectorInfo[] complexInfo) {
    freeDataChunkMemory();
    if (pageCounter >= pageFilteredRowCount.length) {
      return;
    }

    for (int i = 0; i < this.dictionaryColumnChunkIndexes.length; i++) {
      dictionaryInfo[i].vector.setLazyPage(
          new LazyPageLoader(lazyBlockletLoader, dictionaryColumnChunkIndexes[i], false,
              pageIdFiltered[pageCounter], dictionaryInfo[i], dimensionReusableBuffer[i]));
    }
    dimensionReusableBufferIndex = dictionaryColumnChunkIndexes.length;
    for (int i = 0; i < this.noDictionaryColumnChunkIndexes.length; i++) {
      noDictionaryInfo[i].vector.setLazyPage(
          new LazyPageLoader(lazyBlockletLoader, noDictionaryColumnChunkIndexes[i], false,
              pageIdFiltered[pageCounter], noDictionaryInfo[i],
              dimensionReusableBuffer[dimensionReusableBufferIndex++]));
    }
    dimensionReusableBufferIndex =
        noDictionaryColumnChunkIndexes.length + dictionaryColumnChunkIndexes.length;

    for (int i = 0; i < this.complexParentBlockIndexes.length; i++) {
      complexInfo[i].vector.getColumnVector().setLazyPage(
          new LazyPageLoader(lazyBlockletLoader, complexParentBlockIndexes[i], false,
              pageIdFiltered[pageCounter], complexInfo[i],
              dimensionReusableBuffer[dimensionReusableBufferIndex++]));
      // set child pages
      int dimIndex = 0;
      for (CarbonColumnVector childVector : complexInfo[i].vector.getColumnVector()
          .getChildrenVector()) {
        fillChildDataChunks(childVector,
            complexInfo[i].dimension.getDimension().getListOfChildDimensions().get(dimIndex++),
            complexInfo[i]);
      }
    }

    for (int i = 0; i < measuresOrdinal.length; i++) {
      msrVectorInfo[i].vector.setLazyPage(
          new LazyPageLoader(lazyBlockletLoader, measuresOrdinal[i], true,
              pageIdFiltered[pageCounter], msrVectorInfo[i], measureReusableBuffer[i]));
    }

  }

  // free the memory for the last page chunk
  private void freeDataChunkMemory() {
    for (int i = 0; i < dimensionColumnPages.length; i++) {
      if (pageCounter > 0 && dimensionColumnPages[i][pageCounter - 1] != null) {
        dimensionColumnPages[i][pageCounter - 1].freeMemory();
        dimensionColumnPages[i][pageCounter - 1] = null;
      }
    }
    for (int i = 0; i < measureColumnPages.length; i++) {
      if (pageCounter > 0 && measureColumnPages[i][pageCounter - 1] != null) {
        measureColumnPages[i][pageCounter - 1].freeMemory();
        measureColumnPages[i][pageCounter - 1] = null;
      }
    }
    clearValidRowIdList();
  }

  public int numberOfPages() {
    return pageFilteredRowCount.length;
  }

  public int[] getPageIdFiltered() {
    return pageIdFiltered;
  }

  public void setPageIdFiltered(int[] pageIdFiltered) {
    this.pageIdFiltered = pageIdFiltered;
  }

  /**
   * Get total rows in the current page
   *
   * @return
   */
  public int getCurrentPageRowCount() {
    return pageFilteredRowCount[pageCounter];
  }

  public int getCurrentPageCounter() {
    return pageCounter;
  }

  /**
   * increment the counter.
   */
  public void setRowCounter(int rowCounter) {
    this.rowCounter = rowCounter;
  }

  /**
   * Below method will be used to get the dimension key array
   * for all the no dictionary dimension present in the query
   *
   * @param rowId row number
   * @return no dictionary keys for all no dictionary dimension
   */
  protected byte[][] getNoDictionaryKeyArray(int rowId) {
    byte[][] noDictionaryColumnsKeys = new byte[noDictionaryColumnChunkIndexes.length][];
    int position = 0;
    for (int i = 0; i < this.noDictionaryColumnChunkIndexes.length; i++) {
      noDictionaryColumnsKeys[position++] =
          dimensionColumnPages[noDictionaryColumnChunkIndexes[i]][pageCounter].getChunkData(rowId);
    }
    return noDictionaryColumnsKeys;
  }

  /**
   * This method will return the bitsets for valid row Id's to be scanned
   *
   * @param rowId
   * @param batchSize
   * @return
   */

  public abstract void fillValidRowIdsBatchFilling(int rowId, int batchSize);

  protected void clearValidRowIdList() {
    if (null != validRowIds && !validRowIds.isEmpty()) {
      validRowIds.clear();
    }
  }

  public List<Integer> getValidRowIds() {
    return validRowIds;
  }

  /**
   * Below method will be used to get the complex type keys array based
   * on row id for all the complex type dimension selected in query.
   * This method will be used to fill the data column wise
   *
   * @return complex type key array for all the complex dimension selected in query
   */
  protected List<byte[][]> getComplexTypeKeyArrayBatch() {
    List<byte[][]> complexTypeArrayList = new ArrayList<>(validRowIds.size());
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    ReUsableByteArrayDataOutputStream reusableDataOutput =
        new ReUsableByteArrayDataOutputStream(byteStream);
    boolean isExceptionThrown = false;
    byte[][] complexTypeData = null;
    // everyTime it is initialized new as in case of prefetch it can modify the data
    for (int i = 0; i < validRowIds.size(); i++) {
      complexTypeData = new byte[complexParentBlockIndexes.length][];
      complexTypeArrayList.add(complexTypeData);
    }
    for (int i = 0; i < complexParentBlockIndexes.length; i++) {
      // get the genericQueryType for 1st column
      GenericQueryType genericQueryType =
          complexParentIndexToQueryMap.get(complexParentBlockIndexes[i]);
      for (int j = 0; j < validRowIds.size(); j++) {
        try {
          genericQueryType
              .parseBlocksAndReturnComplexColumnByteArray(dimRawColumnChunks, dimensionColumnPages,
                  validRowIds.get(j), pageCounter, reusableDataOutput);
          // get the key array in columnar way
          byte[][] complexKeyArray = complexTypeArrayList.get(j);
          complexKeyArray[i] = byteStream.toByteArray();
          reusableDataOutput.reset();
        } catch (IOException e) {
          isExceptionThrown = true;
          LOGGER.error(e.getMessage(), e);
        } finally {
          if (isExceptionThrown) {
            CarbonUtil.closeStreams(reusableDataOutput);
            CarbonUtil.closeStreams(byteStream);
          }
        }
      }
    }
    CarbonUtil.closeStreams(reusableDataOutput);
    CarbonUtil.closeStreams(byteStream);
    return complexTypeArrayList;
  }

  /**
   * @return blockletId
   */
  public String getBlockletId() {
    return blockletId;
  }

  /**
   * Set blocklet id, which looks like
   * "Part0/Segment_0/part-0-0_batchno0-0-1517155583332.carbondata/0"
   */
  public void setBlockletId(String blockletId, String blockletNumber) {
    this.blockletId = blockletId + CarbonCommonConstants.FILE_SEPARATOR + blockletNumber;
    this.blockletNumber = blockletNumber;
    // if deleted record map is present for this block
    // then get the first page deleted vo
    if (null != deletedRecordMap) {
      String key;
      if (pageIdFiltered != null) {
        key = this.blockletNumber + '_' + pageIdFiltered[pageCounter];
      } else {
        key = this.blockletNumber + '_' + pageCounter;
      }
      currentDeleteDeltaVo = deletedRecordMap.get(key);
    }
  }

  /**
   * Below method will be used to get the complex type keys array based
   * on row id for all the complex type dimension selected in query
   *
   * @param rowId row number
   * @return complex type key array for all the complex dimension selected in query
   */
  protected byte[][] getComplexTypeKeyArray(int rowId) {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    ReUsableByteArrayDataOutputStream reUsableDataOutput =
        new ReUsableByteArrayDataOutputStream(byteStream);
    boolean isExceptionThrown = false;
    byte[][] complexTypeData = new byte[complexParentBlockIndexes.length][];
    for (int i = 0; i < complexTypeData.length; i++) {
      GenericQueryType genericQueryType =
          complexParentIndexToQueryMap.get(complexParentBlockIndexes[i]);
      try {
        genericQueryType
            .parseBlocksAndReturnComplexColumnByteArray(dimRawColumnChunks, dimensionColumnPages,
                rowId, pageCounter, reUsableDataOutput);
        complexTypeData[i] = byteStream.toByteArray();
        reUsableDataOutput.reset();
      } catch (IOException e) {
        isExceptionThrown = true;
        LOGGER.error(e.getMessage(), e);
      } finally {
        if (isExceptionThrown) {
          CarbonUtil.closeStreams(reUsableDataOutput);
          CarbonUtil.closeStreams(byteStream);
        }
      }
    }
    CarbonUtil.closeStreams(reUsableDataOutput);
    CarbonUtil.closeStreams(byteStream);
    return complexTypeData;
  }

  /**
   * to check whether any more row is present in the result
   *
   * @return
   */
  public boolean hasNext() {
    if (pageCounter
        < pageFilteredRowCount.length && rowCounter < this.pageFilteredRowCount[pageCounter]) {
      return true;
    } else if (pageCounter < pageFilteredRowCount.length) {
      pageCounter++;
      if (pageCounter >= pageFilteredRowCount.length) {
        return false;
      }
      if (this.pageFilteredRowCount[pageCounter] == 0) {
        return hasNext();
      }
      fillDataChunks();
      rowCounter = 0;
      currentRow = -1;
      if (null != deletedRecordMap) {
        currentDeleteDeltaVo = deletedRecordMap.get(blockletNumber + "_" + pageCounter);
      }
      return hasNext();
    }
    return false;
  }

  /**
   * Below method will be used to free the occupied memory
   */
  public void freeMemory() {
    // first free the dimension chunks
    if (null != dimensionColumnPages) {
      for (int i = 0; i < dimensionColumnPages.length; i++) {
        if (null != dimensionColumnPages[i]) {
          for (int j = 0; j < dimensionColumnPages[i].length; j++) {
            if (null != dimensionColumnPages[i][j]) {
              dimensionColumnPages[i][j].freeMemory();
              dimensionColumnPages[i][j] = null;
            }
          }
        }
      }
    }
    // free the measure data chunks
    if (null != measureColumnPages) {
      for (int i = 0; i < measureColumnPages.length; i++) {
        if (null != measureColumnPages[i]) {
          for (int j = 0; j < measureColumnPages[i].length; j++) {
            if (null != measureColumnPages[i][j]) {
              measureColumnPages[i][j].freeMemory();
              measureColumnPages[i][j] = null;
            }
          }
        }
      }
    }
    // free the raw chunks
    if (null != dimRawColumnChunks) {
      for (int i = 0; i < dimRawColumnChunks.length; i++) {
        if (null != dimRawColumnChunks[i]) {
          dimRawColumnChunks[i].freeMemory();
          dimRawColumnChunks[i] = null;
        }
      }
    }
    clearValidRowIdList();
    validRowIds = null;
  }

  /**
   * @param pageFilteredRowCount set total of number rows valid after scanning
   */
  public void setPageFilteredRowCount(int[] pageFilteredRowCount) {
    this.pageFilteredRowCount = pageFilteredRowCount;
    if (pageIdFiltered == null) {
      pageIdFiltered = new int[pageFilteredRowCount.length];
      for (int i = 0; i < pageIdFiltered.length; i++) {
        pageIdFiltered[i] = i;
      }
    }
  }

  /**
   * After applying filter it will return the  bit set with the valid row indexes
   * so below method will be used to set the row indexes
   */
  public void setPageFilteredRowId(int[][] pageFilteredRowId) {
    this.pageFilteredRowId = pageFilteredRowId;
  }

  public int getRowCounter() {
    return rowCounter;
  }

  /**
   * will return the current valid row id
   *
   * @return valid row id
   */
  public abstract int getCurrentRowId();

  /**
   * @return dictionary key array for all the dictionary dimension in integer array format
   * selected in query
   */
  public abstract int[] getDictionaryKeyIntegerArray();

  /**
   * Method to fill each dictionary column data column wise
   *
   * @param batchSize
   * @return
   */
  public List<byte[]> getDictionaryKeyArrayBatch(int batchSize) {
    // rowId from where computing need to start
    int startRowId = currentRow + 1;
    fillValidRowIdsBatchFilling(startRowId, batchSize);
    List<byte[]> dictionaryKeyArrayList = new ArrayList<>(validRowIds.size());
    int[] columnDataOffsets = null;
    byte[] completeKey = null;
    // everyTime it is initialized new as in case of prefetch it can modify the data
    for (int i = 0; i < validRowIds.size(); i++) {
      completeKey = new byte[fixedLengthKeySize];
      dictionaryKeyArrayList.add(completeKey);
    }
    // initialize offset array if data is present
    if (this.dictionaryColumnChunkIndexes.length > 0) {
      columnDataOffsets = new int[validRowIds.size()];
    }
    for (int i = 0; i < this.dictionaryColumnChunkIndexes.length; i++) {
      for (int j = 0; j < validRowIds.size(); j++) {
        columnDataOffsets[j] += dimensionColumnPages[dictionaryColumnChunkIndexes[i]][pageCounter]
            .fillRawData(validRowIds.get(j), columnDataOffsets[j], dictionaryKeyArrayList.get(j));
      }
    }
    return dictionaryKeyArrayList;
  }

  /**
   * Below method will be used to get the complex type key array
   *
   * @return complex type key array
   */
  public abstract byte[][] getComplexTypeKeyArray();

  /**
   * Below method will be used to get the complex type key array
   * This method will fill the data column wise for the given batch size
   *
   * @param batchSize
   * @return complex type key array
   */
  public abstract List<byte[][]> getComplexTypeKeyArrayBatch(int batchSize);

  /**
   * Below method will be used to get the no dictionary key
   * array for all the no dictionary dimension selected in query
   *
   * @return no dictionary key array for all the no dictionary dimension
   */
  public abstract byte[][] getNoDictionaryKeyArray();

  /**
   * Below method will be used to get the dimension key array
   * for all the no dictionary dimension present in the query
   * This method will fill the data column wise for the given batch size
   *
   * @return no dictionary keys for all no dictionary dimension
   */
  public List<byte[][]> getNoDictionaryKeyArrayBatch(int batchSize) {
    List<byte[][]> noDictionaryKeyArrayList = new ArrayList<>(validRowIds.size());
    byte[][] noDictionaryColumnsKeys = null;
    // everyTime it is initialized new as in case of prefetch it can modify the data
    for (int i = 0; i < validRowIds.size(); i++) {
      noDictionaryColumnsKeys = new byte[noDictionaryColumnChunkIndexes.length][];
      noDictionaryKeyArrayList.add(noDictionaryColumnsKeys);
    }
    int columnPosition = 0;
    for (int i = 0; i < this.noDictionaryColumnChunkIndexes.length; i++) {
      for (int j = 0; j < validRowIds.size(); j++) {
        byte[][] noDictionaryArray = noDictionaryKeyArrayList.get(j);
        noDictionaryArray[columnPosition] =
            dimensionColumnPages[noDictionaryColumnChunkIndexes[i]][pageCounter]
                .getChunkData(validRowIds.get(j));
      }
      columnPosition++;
    }
    return noDictionaryKeyArrayList;
  }

  /**
   * Mark the filtered rows in columnar batch. These rows will not be added to vector batches later.
   * @param columnarBatch
   * @param startRow
   * @param size
   * @param vectorOffset
   */
  public int markFilteredRows(CarbonColumnarBatch columnarBatch, int startRow, int size,
      int vectorOffset) {
    int rowsFiltered = 0;
    if (currentDeleteDeltaVo != null) {
      int len = startRow + size;
      for (int i = startRow; i < len; i++) {
        int rowId = pageFilteredRowId != null ? pageFilteredRowId[pageCounter][i] : i;
        if (currentDeleteDeltaVo.containsRow(rowId)) {
          columnarBatch.markFiltered(vectorOffset);
          rowsFiltered++;
        }
        vectorOffset++;
      }
    }
    return rowsFiltered;
  }

  public DeleteDeltaVo getCurrentDeleteDeltaVo() {
    return currentDeleteDeltaVo;
  }

  /**
   * Below method will be used to check row got deleted
   *
   * @param rowId
   * @return is present in deleted row
   */
  public boolean containsDeletedRow(int rowId) {
    if (null != currentDeleteDeltaVo) {
      return currentDeleteDeltaVo.containsRow(rowId);
    }
    return false;
  }

  public int getBlockletNumber() {
    return Integer.parseInt(blockletNumber);
  }
}
