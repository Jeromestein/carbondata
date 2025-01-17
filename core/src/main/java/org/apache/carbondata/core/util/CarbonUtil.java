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

package org.apache.carbondata.core.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.block.AbstractIndex;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.columnar.UnBlockIndexer;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.FallbackEncodedColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.DefaultEncodingFactory;
import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;
import org.apache.carbondata.core.exception.InvalidConfigurationException;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.indexstore.BlockletDetailInfo;
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore;
import org.apache.carbondata.core.indexstore.row.IndexRow;
import org.apache.carbondata.core.keygenerator.directdictionary.timestamp.DateDirectDictionaryGenerator;
import org.apache.carbondata.core.localdictionary.generator.ColumnLocalDictionaryGenerator;
import org.apache.carbondata.core.localdictionary.generator.LocalDictionaryGenerator;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypeAdapter;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.SchemaEvolution;
import org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.metadata.schema.table.column.ParentColumnTableRelation;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.reader.CarbonHeaderReader;
import org.apache.carbondata.core.reader.CarbonIndexFileReader;
import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.core.reader.ThriftReader.TBaseCreator;
import org.apache.carbondata.core.scan.model.ProjectionDimension;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.BlockletHeader;
import org.apache.carbondata.format.DataChunk3;
import org.apache.carbondata.format.IndexHeader;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.input.ClassLoaderObjectInputStream;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

public final class CarbonUtil {

  /**
   * Attribute for Carbon LOGGER
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonUtil.class.getName());

  /**
   * EIGHT
   */
  private static final int CONST_EIGHT = 8;

  /**
   * SEVEN
   */
  private static final int CONST_SEVEN = 7;

  /**
   * HUNDRED
   */
  private static final int CONST_HUNDRED = 100;

  /**
   * dfs.bytes-per-checksum
   * HDFS checksum length, block size for a file should be exactly divisible
   * by this value
   */
  private static final int HDFS_CHECKSUM_LENGTH = 512;

  private CarbonUtil() {

  }

  /**
   * This method closes the streams
   *
   * @param streams - streams to close.
   */
  public static void closeStreams(Closeable... streams) {
    // Added if to avoid NullPointerException in case one stream is being passed as null
    if (null != streams) {
      for (Closeable stream : streams) {
        try {
          closeStream(stream);
        } catch (IOException e) {
          LOGGER.error("Error while closing stream:" + e, e);
        }
      }
    }
  }

  /**
   * This method closes stream
   *
   * @param stream
   * @throws IOException
   */
  public static void closeStream(Closeable stream) throws IOException {
    if (null != stream) {
      stream.close();
    }
  }

  /**
   * This method will be used to update the dimension cardinality
   *
   * @param dimCardinality
   * @return new increment cardinality
   */
  public static int[] getIncrementedCardinality(int[] dimCardinality) {
    // get the cardinality incr factor
    final int incrValue = CarbonCommonConstants.CARDINALITY_INCREMENT_VALUE_DEFAULT_VAL;

    int perIncr = 0;
    int remainder = 0;
    int[] newDimsC = new int[dimCardinality.length];
    for (int i = 0; i < dimCardinality.length; i++) {
      // get the incr
      perIncr = (dimCardinality[i] * incrValue) / CONST_HUNDRED;

      // if per incr is more than one the add to cardinality
      if (perIncr > 0) {
        newDimsC[i] = dimCardinality[i] + perIncr;
      } else {
        // else add one
        newDimsC[i] = dimCardinality[i] + 1;
      }
      // check whether its in boundary condition
      remainder = newDimsC[i] % CONST_EIGHT;
      if (remainder == CONST_SEVEN) {
        // then incr cardinality by 1
        newDimsC[i] = dimCardinality[i] + 1;
      }
    }
    // get the log bits of cardinality
    for (int i = 0; i < newDimsC.length; i++) {
      newDimsC[i] = Long.toBinaryString(newDimsC[i]).length();
    }
    return newDimsC;
  }

  public static int getIncrementedCardinality(int dimCardinality) {
    // get the cardinality incr factor
    final int incrValue = CarbonCommonConstants.CARDINALITY_INCREMENT_VALUE_DEFAULT_VAL;

    int perIncr = 0;
    int remainder = 0;
    int newDimsC = 0;

    // get the incr
    perIncr = (dimCardinality * incrValue) / CONST_HUNDRED;

    // if per incr is more than one the add to cardinality
    if (perIncr > 0) {
      newDimsC = dimCardinality + perIncr;
    } else {
      // else add one
      newDimsC = dimCardinality + 1;
    }
    // check whether its in boundary condition
    remainder = newDimsC % CONST_EIGHT;
    if (remainder == CONST_SEVEN) {
      // then incr cardinality by 1
      newDimsC = dimCardinality + 1;
    }
    newDimsC = Long.toBinaryString(newDimsC).length();
    // get the log bits of cardinality

    return newDimsC;
  }

  /**
   * This method will be used to update the dimension cardinality
   *
   * @param dimCardinality
   * @return new increment cardinality
   */
  public static int[] getIncrementedCardinalityFullyFilled(int[] dimCardinality) {
    int[] newDimsC = new int[dimCardinality.length];
    // get the log bits of cardinality
    for (int i = 0; i < dimCardinality.length; i++) {
      if (dimCardinality[i] == 0) {
        //Array or struct type may have higher value
        newDimsC[i] = 64;
      } else {
        int bitsLength = Long.toBinaryString(dimCardinality[i]).length();
        int div = bitsLength / 8;
        int mod = bitsLength % 8;
        if (mod > 0) {
          newDimsC[i] = 8 * (div + 1);
        } else {
          newDimsC[i] = bitsLength;
        }
      }
    }
    return newDimsC;
  }

  private static int getBitLengthFullyFilled(int dimensionLens) {
    int bitsLength = Long.toBinaryString(dimensionLens).length();
    int div = bitsLength / 8;
    int mod = bitsLength % 8;
    if (mod > 0) {
      return 8 * (div + 1);
    } else {
      return bitsLength;
    }
  }

  /**
   * This method will be used to delete the folder and files
   *
   * @param path file path array
   * @throws Exception exception
   */
  public static void deleteFoldersAndFiles(final File... path)
      throws IOException, InterruptedException {
    UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<Void>() {

      @Override
      public Void run() throws Exception {
        for (int i = 0; i < path.length; i++) {
          CarbonFile carbonFile = FileFactory.getCarbonFile(path[i].getAbsolutePath());
          boolean delete = carbonFile.delete();
          if (!delete) {
            throw new IOException("Error while deleting file: " + carbonFile.getAbsolutePath());
          }
        }
        return null;
      }
    });
  }

  public static void deleteFoldersAndFiles(final CarbonFile... file)
      throws IOException, InterruptedException {
    UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<Void>() {

      @Override
      public Void run() throws Exception {
        for (CarbonFile carbonFile : file) {
          if (carbonFile.exists()) {
            boolean delete = carbonFile.delete();
            if (!delete) {
              throw new IOException("Error while deleting file: " + carbonFile.getAbsolutePath());
            }
          }
        }
        return null;
      }
    });
  }

  public static void deleteFoldersAndFilesSilent(final CarbonFile... file)
      throws IOException, InterruptedException {
    UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<Void>() {

      @Override
      public Void run() {
        for (CarbonFile carbonFile : file) {
          if (carbonFile.exists()) {
            boolean delete = carbonFile.delete();
            if (!delete) {
              LOGGER.warn("Unable to delete file: " + carbonFile.getCanonicalPath());
            }
          }
        }
        return null;
      }
    });
  }

  public static void deleteFiles(File[] intermediateFiles) throws IOException {
    for (File intermediateFile : intermediateFiles) {
      // ignore deleting for index file since it is inside merged file.
      if (!intermediateFile.delete() && !intermediateFile.getName()
          .endsWith(CarbonTablePath.INDEX_FILE_EXT)) {
        throw new IOException("Problem while deleting intermediate file");
      }
    }
  }

  public static int getFirstIndexUsingBinarySearch(DimensionColumnPage dimColumnDataChunk,
      int low, int high, byte[] compareValue, boolean matchUpLimit) {
    int cmpResult = 0;
    while (high >= low) {
      int mid = (low + high) / 2;
      cmpResult = dimColumnDataChunk.compareTo(mid, compareValue);
      if (cmpResult < 0) {
        low = mid + 1;
      } else if (cmpResult > 0) {
        high = mid - 1;
      } else {
        int currentIndex = mid;
        if (!matchUpLimit) {
          while (currentIndex - 1 >= 0
              && dimColumnDataChunk.compareTo(currentIndex - 1, compareValue) == 0) {
            --currentIndex;
          }
        } else {
          while (currentIndex + 1 <= high
              && dimColumnDataChunk.compareTo(currentIndex + 1, compareValue) == 0) {
            currentIndex++;
          }
        }
        return currentIndex;
      }
    }
    return -(low + 1);
  }

  /**
   * search a specific compareValue's range index in a sorted byte array
   *
   * @param dimColumnDataChunk
   * @param low
   * @param high
   * @param compareValue
   * @return the compareValue's range index in the dimColumnDataChunk
   */
  public static int[] getRangeIndexUsingBinarySearch(
      DimensionColumnPage dimColumnDataChunk, int low, int high, byte[] compareValue) {

    int[] rangeIndex = new int[2];
    int cmpResult = 0;
    while (high >= low) {
      int mid = (low + high) / 2;
      cmpResult = dimColumnDataChunk.compareTo(mid, compareValue);
      if (cmpResult < 0) {
        low = mid + 1;
      } else if (cmpResult > 0) {
        high = mid - 1;
      } else {

        int currentIndex = mid;
        while (currentIndex - 1 >= 0
            && dimColumnDataChunk.compareTo(currentIndex - 1, compareValue) == 0) {
          --currentIndex;
        }
        rangeIndex[0] = currentIndex;

        currentIndex = mid;
        while (currentIndex + 1 <= high
            && dimColumnDataChunk.compareTo(currentIndex + 1, compareValue) == 0) {
          currentIndex++;
        }
        rangeIndex[1] = currentIndex;

        return rangeIndex;
      }
    }

    // key not found. return a not exist range
    // rangeIndex[0] = 0;
    rangeIndex[1] = -1;
    return rangeIndex;
  }

  /**
   * Checks that {@code fromIndex} and {@code toIndex} are in the range and
   * throws an exception if they aren't.
   */
  private static void rangeCheck(int fromIndex, int toIndex) {
    if (fromIndex > toIndex) {
      throw new IllegalArgumentException("fromIndex(" + fromIndex + ") > toIndex(" + toIndex + ")");
    }
    if (fromIndex < 0) {
      throw new ArrayIndexOutOfBoundsException(fromIndex);
    }
  }

  /**
   * search a specific key in sorted byte array
   *
   * @param filterValues
   * @param low
   * @param high
   * @return the compareValue's index in the filterValues
   */
  public static int binarySearch(byte[][] filterValues, int low, int high,
      DimensionColumnPage dimensionColumnPage, int rowId) {
    rangeCheck(low, high);
    while (low <= high) {
      int mid = (low + high) >>> 1;
      int result = dimensionColumnPage.compareTo(rowId, filterValues[mid]);
      if (result < 0) {
        high = mid - 1;
      } else if (result > 0) {
        low = mid + 1;
      } else {
        return mid; // key found
      }

    }
    // key not found
    return -(low + 1);
  }

  /**
   * Method will identify the value which is lesser than the pivot element
   * on which range filter is been applied.
   *
   * @param currentIndex
   * @param dimColumnDataChunk
   * @param compareValue
   * @return index value
   */
  public static int nextLesserValueToTarget(int currentIndex,
      DimensionColumnPage dimColumnDataChunk, byte[] compareValue) {
    while (currentIndex - 1 >= 0
        && dimColumnDataChunk.compareTo(currentIndex - 1, compareValue) >= 0) {
      --currentIndex;
    }

    return --currentIndex;
  }

  /**
   * Method will identify the value which is greater than the pivot element
   * on which range filter is been applied.
   *
   * @param currentIndex
   * @param dimColumnDataChunk
   * @param compareValue
   * @param numberOfRows
   * @return index value
   */
  public static int nextGreaterValueToTarget(int currentIndex,
      DimensionColumnPage dimColumnDataChunk, byte[] compareValue, int numberOfRows) {
    while (currentIndex + 1 < numberOfRows
        && dimColumnDataChunk.compareTo(currentIndex + 1, compareValue) <= 0) {
      ++currentIndex;
    }

    return ++currentIndex;
  }

  public static int[] getUnCompressColumnIndex(int totalLength, ByteBuffer buffer, int offset) {
    buffer.position(offset);
    int indexDataLength = buffer.getInt();
    int indexMapLength = totalLength - indexDataLength - CarbonCommonConstants.INT_SIZE_IN_BYTE;
    int[] indexData = getIntArray(buffer, buffer.position(), indexDataLength);
    int[] indexMap = getIntArray(buffer, buffer.position(), indexMapLength);
    return UnBlockIndexer.uncompressIndex(indexData, indexMap);
  }

  public static int[] getIntArray(ByteBuffer data, int offset, int length) {
    if (length == 0) {
      return new int[0];
    }
    data.position(offset);
    int[] intArray = new int[length / 2];
    int index = 0;
    while (index < intArray.length) {
      intArray[index++] = data.getShort();
    }
    return intArray;
  }

  /**
   * Convert int array to Integer list
   *
   * @param array
   * @return List<Integer>
   */
  public static List<Integer> convertToIntegerList(int[] array) {
    List<Integer> integers = new ArrayList<Integer>();
    for (int i = 0; i < array.length; i++) {
      integers.add(array[i]);
    }
    return integers;
  }

  /**
   * From beeline if a delimiter is passed as \001, in code we get it as
   * escaped string as \\001. So this method will unescape the slash again and
   * convert it back to \001
   *
   * @param parseStr
   * @return
   */
  public static String unescapeChar(String parseStr) {
    return StringEscapeUtils.unescapeJava(parseStr);
  }

  /**
   * remove the quote char for a string, e.g. "abc" => abc, 'abc' => abc
   * @param parseStr
   * @return
   */
  public static String unquoteChar(String parseStr) {
    if (parseStr == null) {
      return null;
    }
    if (parseStr.startsWith("'") && parseStr.endsWith("'")) {
      return parseStr.substring(1, parseStr.length() - 1);
    } else if (parseStr.startsWith("\"") && parseStr.endsWith("\"")) {
      return parseStr.substring(1, parseStr.length() - 1);
    } else {
      return parseStr;
    }
  }

  /**
   * special char delimiter Converter
   *
   * @param delimiter
   * @return delimiter
   */
  public static String delimiterConverter(String delimiter) {
    switch (delimiter) {
      case "\\001":
      case "\\002":
      case "\\003":
      case "\\004":
      case "|":
      case "*":
      case ".":
      case ":":
      case "^":
      case "\\":
      case "$":
      case "+":
      case "?":
      case "(":
      case ")":
      case "{":
      case "}":
      case "[":
      case "]":
        return "\\" + delimiter;
      default:
        return delimiter;
    }
  }

  /**
   * Append HDFS Base Url for show create & load data sql
   *
   * @param filePath
   */
  public static String checkAndAppendHDFSUrl(String filePath) {
    String currentPath = filePath;
    String defaultFsUrl = FileFactory.getConfiguration().get(CarbonCommonConstants.FS_DEFAULT_FS);
    String baseDFSUrl = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_DDL_BASE_HDFS_URL, "");
    if (FileFactory.checkIfPrefixExists(filePath)) {
      return currentPath;
    }
    if (baseDFSUrl.endsWith("/")) {
      baseDFSUrl = baseDFSUrl.substring(0, baseDFSUrl.length() - 1);
    }
    if (!filePath.startsWith("/")) {
      filePath = "/" + filePath;
    }
    currentPath = baseDFSUrl + filePath;
    if (FileFactory.checkIfPrefixExists(currentPath)) {
      return currentPath;
    }
    if (defaultFsUrl == null) {
      return currentPath;
    }
    return defaultFsUrl + currentPath;
  }

  /**
   * Append default file system schema if not added to the filepath
   *
   * @param filePath
   */
  public static String checkAndAppendFileSystemURIScheme(String filePath) {
    String currentPath = filePath;

    if (FileFactory.checkIfPrefixExists(filePath)) {
      return currentPath;
    }
    if (!filePath.startsWith("/")) {
      filePath = "/" + filePath;
    }
    currentPath = filePath;
    String defaultFsUrl = FileFactory.getConfiguration().get(CarbonCommonConstants.FS_DEFAULT_FS);
    if (defaultFsUrl == null) {
      return currentPath;
    }
    return defaultFsUrl + currentPath;
  }

  /**
   * infer compress name from file name
   * @param path file name
   * @return compressor name
   */
  public static String inferCompressorFromFileName(String path) {
    if (path.endsWith(".gz")) {
      return "GZIP";
    } else if (path.endsWith("bz2")) {
      return "BZIP2";
    } else {
      return "";
    }
  }

  public static String removeAKSK(String filePath) {

    if (null == filePath) {
      return "";
    }
    String lowerPath = filePath.toLowerCase(Locale.getDefault());
    if (lowerPath.startsWith(CarbonCommonConstants.S3N_PREFIX) ||
        lowerPath.startsWith(CarbonCommonConstants.S3A_PREFIX) ||
        lowerPath.startsWith(CarbonCommonConstants.S3_PREFIX)) {
      int prefixLength = filePath.indexOf(":") + 3;
      int pathOffset = filePath.indexOf("@");
      if (pathOffset > 0) {
        return filePath.substring(0, prefixLength) + filePath.substring(pathOffset + 1);
      }
    }
    return filePath;
  }

  /**
   * This method will check the existence of a file at a given path
   */
  public static boolean isFileExists(String fileName) {
    try {
      if (FileFactory.isFileExist(fileName)) {
        return true;
      }
    } catch (IOException e) {
      LOGGER.error("@@@@@@  File not found at a given location @@@@@@ : " + removeAKSK(fileName));
    }
    return false;
  }

  /**
   * This method will check and create the given path
   */
  public static boolean checkAndCreateFolder(String path) {
    boolean created = false;
    try {
      if (FileFactory.isFileExist(path)) {
        created = true;
      } else {
        created = FileFactory.mkdirs(path);
      }
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
    }
    return created;
  }

  /**
   *
   * This method will check and create the given path with 777 permission
   */
  public static boolean checkAndCreateFolderWithPermission(String path) {
    boolean created = false;
    try {
      if (FileFactory.isFileExist(path)) {
        created = true;
      } else {
        FileFactory.createDirectoryAndSetPermission(path,
            new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
        created = true;
      }
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
    }
    return created;
  }

  /**
   * This method will return the size of a given file
   */
  public static long getFileSize(String filePath) {
    CarbonFile carbonFile = FileFactory.getCarbonFile(filePath);
    return carbonFile.getSize();
  }

  /**
   * This method will be used to get bit length of the dimensions based on the
   * dimension partitioner. If partitioner is value is 1 the column
   * cardinality will be incremented in such a way it will fit in byte level.
   * for example if number of bits required to store one column value is 3
   * bits the 8 bit will be assigned to each value of that column.In this way
   * we may waste some bits(maximum 7 bits) If partitioner value is more than
   * 1 then few column are stored together. so cardinality of that group will
   * be incremented to fit in byte level For example: if cardinality for 3
   * columns stored together is [1,1,1] then number of bits required will be
   * [1,1,1] then last value will be incremented and it will become[1,1,6]
   *
   * @param dimCardinality cardinality of each column
   * @param dimPartitioner Partitioner is how column is stored if value is 1 then column
   *                       wise if value is more than 1 then it is in group with other
   *                       column
   * @return number of bits for each column
   * @TODO for row group only last value is incremented problem in this cases
   * in if last column in that group is selected most of the time in
   * filter query Comparison will be more if it incremented uniformly
   * then comparison will be distributed
   */
  public static int[] getDimensionBitLength(int[] dimCardinality, int[] dimPartitioner) {
    int[] bitLength = new int[dimCardinality.length];
    int dimCounter = 0;
    for (int value : dimPartitioner) {
      if (value == 1) {
        // for columnar store
        // fully filled bits means complete byte or number of bits
        // assigned will be in
        // multiplication of 8
        bitLength[dimCounter] = getBitLengthFullyFilled(dimCardinality[dimCounter]);
        dimCounter++;
      } else {
        // for row store
        int totalSize = 0;
        for (int j = 0; j < value; j++) {
          bitLength[dimCounter] = getIncrementedCardinality(dimCardinality[dimCounter]);
          totalSize += bitLength[dimCounter];
          dimCounter++;
        }
        // below code is to increment in such a way that row group will
        // be stored
        // as byte level
        int mod = totalSize % 8;
        if (mod > 0) {
          bitLength[dimCounter - 1] = bitLength[dimCounter - 1] + (8 - mod);
        }
      }
    }
    return bitLength;
  }

  /**
   * Below method will be used to check whether particular encoding is present
   * in the dimension or not
   *
   * @param encoding encoding to search
   * @return if encoding is present in dimension
   */
  public static boolean hasEncoding(List<Encoding> encodings, Encoding encoding) {
    return encodings.contains(encoding);
  }

  /**
   * below method is to check whether data type is present in the data type array
   *
   * @param dataType  data type to be searched
   * @param dataTypes all data types
   * @return if data type is present
   */
  public static boolean hasDataType(DataType dataType, DataType[] dataTypes) {
    for (int i = 0; i < dataTypes.length; i++) {
      if (dataType.equals(dataTypes[i])) {
        return true;
      }
    }
    return false;
  }

  public static boolean[] getImplicitColumnArray(ProjectionDimension[] queryDimensions) {
    boolean[] implicitColumnArray = new boolean[queryDimensions.length];
    for (int i = 0; i < queryDimensions.length; i++) {
      implicitColumnArray[i] = queryDimensions[i].getDimension().hasEncoding(Encoding.IMPLICIT);
    }
    return implicitColumnArray;
  }

  public static boolean[] getComplexDataTypeArray(ProjectionDimension[] queryDimensions) {
    boolean[] dictionaryEncodingArray = new boolean[queryDimensions.length];
    for (int i = 0; i < queryDimensions.length; i++) {
      dictionaryEncodingArray[i] =
          queryDimensions[i].getDimension().getDataType().isComplexType();
    }
    return dictionaryEncodingArray;
  }

  /**
   * Below method will be used to read the data file metadata
   */
  public static DataFileFooter readMetadataFile(TableBlockInfo tableBlockInfo) throws IOException {
    return getDataFileFooter(tableBlockInfo, false);
  }

  /**
   * Below method will be used to read the data file metadata
   *
   * @param tableBlockInfo
   * @param forceReadDataFileFooter flag to decide whether to read the footer of
   *                                carbon data file forcefully
   * @return
   * @throws IOException
   */
  public static DataFileFooter readMetadataFile(TableBlockInfo tableBlockInfo,
      boolean forceReadDataFileFooter) throws IOException {
    return getDataFileFooter(tableBlockInfo, forceReadDataFileFooter);
  }

  private static DataFileFooter getDataFileFooter(TableBlockInfo tableBlockInfo,
      boolean forceReadDataFileFooter) throws IOException {
    BlockletDetailInfo detailInfo = tableBlockInfo.getDetailInfo();
    if (detailInfo == null || forceReadDataFileFooter) {
      if (tableBlockInfo.getBlockOffset() == 0) {
        // caller does not set the footer offset, so read index file and convert to footer
        return getDataFileFooterFromIndexFile(tableBlockInfo);
      } else {
        AbstractDataFileFooterConverter fileFooterConverter =
            DataFileFooterConverterFactory.getInstance()
                .getDataFileFooterConverter(tableBlockInfo.getVersion());
        return fileFooterConverter.readDataFileFooter(tableBlockInfo);
      }
    } else {
      DataFileFooter fileFooter = new DataFileFooter();
      fileFooter.setSchemaUpdatedTimeStamp(detailInfo.getSchemaUpdatedTimeStamp());
      ColumnarFormatVersion version = ColumnarFormatVersion.valueOf(detailInfo.getVersionNumber());
      AbstractDataFileFooterConverter dataFileFooterConverter =
          DataFileFooterConverterFactory.getInstance().getDataFileFooterConverter(version);
      List<ColumnSchema> schema = dataFileFooterConverter.getSchema(tableBlockInfo);
      fileFooter.setColumnInTable(schema);
      return fileFooter;
    }
  }

  /**
   * Read index file and convert to footer.
   * Currently, this is used only in INSERT STAGE command to load data files written by SDK
   */
  private static DataFileFooter getDataFileFooterFromIndexFile(TableBlockInfo tableBlockInfo)
      throws IOException {
    String filePath = tableBlockInfo.getFilePath();
    String dataFilePath = tableBlockInfo.getFilePath();
    String shardName = CarbonTablePath.getShardName(filePath);
    String indexFilePath = String
        .format("%s/%s%s", CarbonTablePath.getParentPath(filePath), shardName,
            CarbonCommonConstants.UPDATE_INDEX_FILE_EXT);
    ColumnarFormatVersion version = ColumnarFormatVersion.valueOf((short)3);
    AbstractDataFileFooterConverter footerConverter =
        DataFileFooterConverterFactory.getInstance().getDataFileFooterConverter(version);
    List<DataFileFooter> footers = footerConverter.getIndexInfo(indexFilePath, null, true);
    DataFileFooter blockFooter = null;
    // find the footer of the input data file (tableBlockInfo)
    for (DataFileFooter blockletFooter : footers) {
      if (blockletFooter.getBlockInfo().getFilePath().equals(dataFilePath)) {
        if (blockFooter == null) {
          blockFooter = blockletFooter;
        } else {
          blockFooter.getBlockletList().addAll(blockletFooter.getBlockletList());
        }
      }
    }
    if (blockFooter == null) {
      throw new RuntimeException("Footer not found in index file");
    } else {
      return blockFooter;
    }
  }

  /**
   * Below method will be used to get the number of dimension column
   * in carbon column schema
   *
   * @param columnSchemaList column schema list
   * @return number of dimension column
   */
  public static int getNumberOfDimensionColumns(List<ColumnSchema> columnSchemaList) {
    int numberOfDimensionColumns = 0;
    ColumnSchema columnSchema = null;
    for (int i = 0; i < columnSchemaList.size(); i++) {
      columnSchema = columnSchemaList.get(i);
      if (columnSchema.isDimensionColumn()) {
        numberOfDimensionColumns++;
      } else {
        break;
      }
    }
    return numberOfDimensionColumns;
  }

  /**
   * Below method will be used to get the surrogate key
   *
   * @param data   actual data
   * @param buffer byte buffer which will be used to convert the data to integer value
   * @return surrogate key
   */
  public static int getSurrogateKey(byte[] data, ByteBuffer buffer) {
    int length = 4 - data.length;
    for (int i = 0; i < length; i++) {
      buffer.put((byte) 0);
    }
    buffer.put(data);
    buffer.rewind();
    int surrogate = buffer.getInt();
    buffer.clear();
    return surrogate;
  }

  /**
   * This method will clear the B-Tree Cache in executors for the given list of blocks
   *
   * @param dataBlocks
   */
  public static void clearBlockCache(List<AbstractIndex> dataBlocks) {
    if (null != dataBlocks) {
      for (AbstractIndex blocks : dataBlocks) {
        blocks.clear();
      }
    }
  }

  /**
   * Below method will be used to get the dimension
   *
   * @param tableDimensionList table dimension list
   * @return boolean array specifying true if dimension is dictionary
   * and false if dimension is not a dictionary column
   */
  public static boolean[] identifyDimensionType(List<CarbonDimension> tableDimensionList) {
    List<Boolean> isDictionaryDimensions = new ArrayList<Boolean>();
    for (CarbonDimension carbonDimension : tableDimensionList) {
      List<CarbonDimension> children = carbonDimension.getListOfChildDimensions();
      //assuming complex dimensions will always be at last
      if (null != children && children.size() > 0) {
        break;
      }
      isDictionaryDimensions.add(carbonDimension.getDataType() == DataTypes.DATE);
    }
    return ArrayUtils
        .toPrimitive(isDictionaryDimensions.toArray(new Boolean[isDictionaryDimensions.size()]));
  }

  public static int compareMeasureData(byte[] first, byte[] second, DataType dataType) {
    ByteBuffer firstBuffer = null;
    ByteBuffer secondBuffer = null;
    if (dataType == DataTypes.BOOLEAN || dataType == DataTypes.BYTE) {
      if (first[0] > second[0]) {
        return 1;
      } else if (first[0] < second[0]) {
        return -1;
      }
      return 0;
    } else if (dataType == DataTypes.DOUBLE) {
      double firstValue = ((ByteBuffer) (ByteBuffer.allocate(8).put(first).flip())).getDouble();
      double secondValue = ((ByteBuffer) (ByteBuffer.allocate(8).put(second).flip())).getDouble();
      if (firstValue > secondValue) {
        return 1;
      } else if (firstValue < secondValue) {
        return -1;
      }
      return 0;
    } else if (dataType == DataTypes.FLOAT) {
      float firstValue = ((ByteBuffer) (ByteBuffer.allocate(8).put(first).flip())).getFloat();
      float secondValue = ((ByteBuffer) (ByteBuffer.allocate(8).put(second).flip())).getFloat();
      if (firstValue > secondValue) {
        return 1;
      } else if (firstValue < secondValue) {
        return -1;
      }
      return 0;
    } else if (dataType == DataTypes.LONG || dataType == DataTypes.INT
            || dataType == DataTypes.SHORT) {
      long firstValue = ((ByteBuffer) (ByteBuffer.allocate(8).put(first).flip())).getLong();
      long secondValue = ((ByteBuffer) (ByteBuffer.allocate(8).put(second).flip())).getLong();
      if (firstValue > secondValue) {
        return 1;
      } else if (firstValue < secondValue) {
        return -1;
      }
      return 0;
    } else if (DataTypes.isDecimal(dataType)) {
      return DataTypeUtil.byteToBigDecimal(first).compareTo(DataTypeUtil.byteToBigDecimal(second));
    } else {
      throw new IllegalArgumentException("Invalid data type:" + dataType);
    }
  }

  /**
   * This method will form one single byte [] for all the high card dims.
   * First it will add all the indexes of variable length byte[] and then the
   * actual value
   *
   * @param byteBufferArr
   * @return byte[] key.
   */
  public static byte[] packByteBufferIntoSingleByteArray(ByteBuffer[] byteBufferArr) {
    // for empty array means there is no data to remove dictionary.
    if (null == byteBufferArr || byteBufferArr.length == 0) {
      return null;
    }
    int noOfCol = byteBufferArr.length;
    short offsetLen = (short) (noOfCol * 2);
    int totalBytes = calculateTotalBytes(byteBufferArr) + offsetLen;
    ByteBuffer buffer = ByteBuffer.allocate(totalBytes);
    // writing the offset of the first element.
    buffer.putShort(offsetLen);

    // prepare index for byte []
    for (int index = 0; index < byteBufferArr.length - 1; index++) {
      ByteBuffer individualCol = byteBufferArr[index];
      int noOfBytes = individualCol.capacity();
      buffer.putShort((short) (offsetLen + noOfBytes));
      offsetLen += noOfBytes;
      individualCol.rewind();
    }

    // put actual data.
    for (int index = 0; index < byteBufferArr.length; index++) {
      ByteBuffer individualCol = byteBufferArr[index];
      buffer.put(individualCol.array());
    }

    buffer.rewind();
    return buffer.array();

  }

  /**
   * To calculate the total bytes in byte Buffer[].
   *
   * @param byteBufferArr
   * @return
   */
  private static int calculateTotalBytes(ByteBuffer[] byteBufferArr) {
    int total = 0;
    for (int index = 0; index < byteBufferArr.length; index++) {
      total += byteBufferArr[index].capacity();
    }
    return total;
  }

  /**
   * Find the dimension from metadata by using unique name. As of now we are
   * taking level name as unique name. But user needs to give one unique name
   * for each level,that level he needs to mention in query.
   *
   * @param dimensions
   * @param carbonDim
   * @return
   */
  public static CarbonDimension findDimension(List<CarbonDimension> dimensions, String carbonDim) {
    CarbonDimension findDim = null;
    for (CarbonDimension dimension : dimensions) {
      if (dimension.getColName().equalsIgnoreCase(carbonDim)) {
        findDim = dimension;
        break;
      }
    }
    return findDim;
  }

  /**
   * This method will search for a given dimension in the current block dimensions list
   *
   * @param blockDimensions
   * @param dimensionToBeSearched
   * @return
   */
  public static CarbonDimension getDimensionFromCurrentBlock(
      List<CarbonDimension> blockDimensions, CarbonDimension dimensionToBeSearched) {
    CarbonDimension currentBlockDimension = null;
    for (CarbonDimension blockDimension : blockDimensions) {
      // In case of SDK, columnId is same as columnName therefore the following check will
      // ensure that if the dimensions columnName is same as the block columnName and the dimension
      // columnId is the same as dimensions columnName then it's a valid column to be scanned.
      if (dimensionToBeSearched.getColumnId().equalsIgnoreCase(blockDimension.getColumnId())
          || blockDimension.isColumnMatchBasedOnId(dimensionToBeSearched)) {
        currentBlockDimension = blockDimension;
        break;
      }
    }
    return currentBlockDimension;
  }

  /**
   * This method will search for a given measure in the current block measures list
   *
   * @param blockMeasures
   * @param measureToBeSearched
   * @return
   */
  public static CarbonMeasure getMeasureFromCurrentBlock(List<CarbonMeasure> blockMeasures,
      CarbonMeasure measureToBeSearched) {
    CarbonMeasure currentBlockMeasure = null;
    for (CarbonMeasure blockMeasure : blockMeasures) {
      // In case of SDK, columnId is same as columnName therefore the following check will
      // ensure that if the measures columnName is same as the block columnName and the measures
      // columnId is the same as measures columnName then it's a valid column to be scanned.
      if (measureToBeSearched.getColumnId().equalsIgnoreCase(blockMeasure.getColumnId())
          || blockMeasure.isColumnMatchBasedOnId(measureToBeSearched)) {
        currentBlockMeasure = blockMeasure;
        break;
      }
    }
    return currentBlockMeasure;
  }

  public static List<ColumnSchema> getColumnSchemaList(List<CarbonDimension> carbonDimensionsList,
      List<CarbonMeasure> carbonMeasureList) {
    List<ColumnSchema> wrapperColumnSchemaList = new ArrayList<ColumnSchema>();
    fillColumnSchemaListForComplexDims(carbonDimensionsList, wrapperColumnSchemaList);
    for (CarbonMeasure carbonMeasure : carbonMeasureList) {
      wrapperColumnSchemaList.add(carbonMeasure.getColumnSchema());
    }
    return wrapperColumnSchemaList;
  }

  private static void fillColumnSchemaListForComplexDims(
      List<CarbonDimension> carbonDimensionsList, List<ColumnSchema> wrapperColumnSchemaList) {
    for (CarbonDimension carbonDimension : carbonDimensionsList) {
      wrapperColumnSchemaList.add(carbonDimension.getColumnSchema());
      List<CarbonDimension> childDims = carbonDimension.getListOfChildDimensions();
      if (null != childDims && childDims.size() > 0) {
        fillColumnSchemaListForComplexDims(childDims, wrapperColumnSchemaList);
      }
    }
  }

  /**
   * initialize the value of dictionary chunk that can be kept in memory at a time
   *
   * @return
   */
  public static int getDictionaryChunkSize() {
    int dictionaryOneChunkSize = 0;
    try {
      dictionaryOneChunkSize = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.DICTIONARY_ONE_CHUNK_SIZE,
              CarbonCommonConstants.DICTIONARY_ONE_CHUNK_SIZE_DEFAULT));
    } catch (NumberFormatException e) {
      dictionaryOneChunkSize =
          Integer.parseInt(CarbonCommonConstants.DICTIONARY_ONE_CHUNK_SIZE_DEFAULT);
      LOGGER.error("Dictionary chunk size not configured properly. Taking default size "
          + dictionaryOneChunkSize);
    }
    return dictionaryOneChunkSize;
  }

  /**
   * @param csvFilePath
   * @return
   */
  public static String readHeader(String csvFilePath) throws IOException {

    DataInputStream fileReader = null;
    BufferedReader bufferedReader = null;
    String readLine = null;

    try {
      fileReader =
          FileFactory.getDataInputStream(csvFilePath);
      bufferedReader = new BufferedReader(new InputStreamReader(fileReader,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));
      readLine = bufferedReader.readLine();
    } finally {
      CarbonUtil.closeStreams(fileReader, bufferedReader);
    }
    return readLine;
  }

  public static String readHeader(String csvFilePath,
      Configuration hadoopConf) throws IOException {

    DataInputStream fileReader = null;
    BufferedReader bufferedReader = null;
    String readLine = null;

    try {
      fileReader = FileFactory.getDataInputStream(
          csvFilePath, -1, hadoopConf);
      bufferedReader = new BufferedReader(new InputStreamReader(fileReader,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));
      readLine = bufferedReader.readLine();
    } finally {
      CarbonUtil.closeStreams(fileReader, bufferedReader);
    }
    return readLine;
  }

  /**
   * Below method will create string like "***********"
   *
   * @param a
   * @param num
   */
  public static String printLine(String a, int num) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < num; i++) {
      builder.append(a);
    }
    return builder.toString();
  }

  /**
   * append a string with left pad to the string builder
   */
  public static void leftPad(StringBuilder builder, String a, int length, char pad) {
    if (builder == null || a == null) {
      return;
    }
    int padLength = length - a.length();
    if (padLength > 0) {
      for (int i = 0; i < padLength; i++) {
        builder.append(pad);
      }
    }
    if (a.length() > 0) {
      builder.append(a);
    }
  }

  /**
   * append a string with right pad to the string builder
   */
  public static void rightPad(StringBuilder builder, String a, int length, char pad) {
    if (builder == null || a == null) {
      return;
    }
    int padLength = length - a.length();
    if (a.length() > 0) {
      builder.append(a);
    }
    if (padLength > 0) {
      for (int i = 0; i < padLength; i++) {
        builder.append(pad);
      }
    }
  }

  /**
   * log information as table
   */
  public static void logTable(StringBuilder builder, String[] header, String[][] rows,
      String indent) {
    int numOfRows = rows.length;
    int numOfColumns = header.length;

    // calculate max length of each column
    int[] maxLengths = new int[numOfColumns];
    for (int columnIndex = 0; columnIndex < numOfColumns; columnIndex++) {
      maxLengths[columnIndex] = header[columnIndex].length();
    }
    for (int rowIndex = 0; rowIndex < numOfRows; rowIndex++) {
      for (int columnIndex = 0; columnIndex < numOfColumns; columnIndex++) {
        maxLengths[columnIndex] =
            Math.max(maxLengths[columnIndex], rows[rowIndex][columnIndex].length());
      }
    }

    // build line
    StringBuilder line = new StringBuilder("+");
    for (int columnIndex = 0; columnIndex < numOfColumns; columnIndex++) {
      CarbonUtil.leftPad(line, "", maxLengths[columnIndex], '-');
      line.append("+");
    }

    // append head
    builder.append(indent).append(line).append("\n").append(indent).append("|");
    for (int columnIndex = 0; columnIndex < numOfColumns; columnIndex++) {
      CarbonUtil.rightPad(builder, header[columnIndex], maxLengths[columnIndex], ' ');
      builder.append("|");
    }
    builder.append("\n").append(indent).append(line);

    // append rows
    for (int rowIndex = 0; rowIndex < numOfRows; rowIndex++) {
      builder.append("\n").append(indent).append("|");
      for (int columnIndex = 0; columnIndex < numOfColumns; columnIndex++) {
        CarbonUtil.leftPad(builder, rows[rowIndex][columnIndex], maxLengths[columnIndex], ' ');
        builder.append("|");
      }
      builder.append("\n").append(indent).append(line);
    }
  }

  public static void logTable(StringBuilder builder, String context, String indent) {
    String[] rows = context.split("\n");
    int maxLength = 0;
    for (String row: rows) {
      maxLength = Math.max(maxLength, row.length());
    }
    StringBuilder line = new StringBuilder("+");
    CarbonUtil.rightPad(line, "", maxLength, '-');
    line.append("+");

    builder.append(indent).append(line);
    for (String row: rows) {
      builder.append("\n").append(indent).append("|");
      CarbonUtil.rightPad(builder, row, maxLength, ' ');
      builder.append("|");
    }
    builder.append("\n").append(indent).append(line);
  }

  /**
   * Below method will be used to get the list of values in
   * comma separated string format
   *
   * @param values
   * @return comma separated segment string
   */
  public static String convertToString(List<Segment> values) {
    if (values == null || values.isEmpty()) {
      return "";
    }
    StringBuilder segmentStringBuilder = new StringBuilder();
    for (int i = 0; i < values.size() - 1; i++) {
      segmentStringBuilder.append(values.get(i));
      segmentStringBuilder.append(",");
    }
    segmentStringBuilder.append(values.get(values.size() - 1));
    return segmentStringBuilder.toString();
  }

  /**
   * Below method will be used to convert the thrift object to byte array.
   */
  public static byte[] getByteArray(TBase t) {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    byte[] thriftByteArray = null;
    TProtocol binaryOut = new TCompactProtocol(new TIOStreamTransport(stream));
    try {
      t.write(binaryOut);
      stream.flush();
      thriftByteArray = stream.toByteArray();
    } catch (TException | IOException e) {
      LOGGER.error("Error while converting to byte array from thrift object: " + e.getMessage(), e);
      closeStreams(stream);
    } finally {
      closeStreams(stream);
    }
    return thriftByteArray;
  }

  public static BlockletHeader readBlockletHeader(byte[] data) throws IOException {
    return (BlockletHeader) read(data, BlockletHeader::new, 0, data.length);
  }

  public static DataChunk3 readDataChunk3(ByteBuffer dataChunkBuffer, int offset, int length)
      throws IOException {
    byte[] data = dataChunkBuffer.array();
    return (DataChunk3) read(data, DataChunk3::new, offset, length);
  }

  public static DataChunk3 readDataChunk3(InputStream stream) throws IOException {
    TBaseCreator creator = DataChunk3::new;
    TProtocol binaryIn = new TCompactProtocol(new TIOStreamTransport(stream));
    TBase t = creator.create();
    try {
      t.read(binaryIn);
    } catch (TException e) {
      throw new IOException(e);
    }
    return (DataChunk3) t;
  }

  /**
   * Below method will be used to convert the byte array value to thrift object for
   * data chunk
   *
   * @param data    thrift byte array
   * @param creator type of thrift
   * @return thrift object
   * @throws IOException any problem while converting the object
   */
  private static TBase read(byte[] data, TBaseCreator creator, int offset, int length)
      throws IOException {
    ByteArrayInputStream stream = new ByteArrayInputStream(data, offset, length);
    TProtocol binaryIn = new TCompactProtocol(new TIOStreamTransport(stream));
    TBase t = creator.create();
    try {
      t.read(binaryIn);
    } catch (TException e) {
      throw new IOException(e);
    } finally {
      CarbonUtil.closeStreams(stream);
    }
    return t;
  }

  /**
   * Below method will be used to convert the apply metadata to
   * ValueEncoderMeta object
   *
   * @param encoderMeta
   * @return ValueEncoderMeta object
   */
  public static ValueEncoderMeta deserializeEncoderMetaV2(byte[] encoderMeta) {
    // TODO : should remove the unnecessary fields.
    ByteArrayInputStream aos = null;
    ObjectInputStream objStream = null;
    ValueEncoderMeta meta = null;
    try {
      aos = new ByteArrayInputStream(encoderMeta);
      objStream =
          new ClassLoaderObjectInputStream(Thread.currentThread().getContextClassLoader(), aos);
      meta = (ValueEncoderMeta) objStream.readObject();
    } catch (ClassNotFoundException e) {
      LOGGER.error(e.getMessage(), e);
    } catch (IOException e) {
      CarbonUtil.closeStreams(objStream);
    }
    return meta;
  }

  public static ValueEncoderMeta deserializeEncoderMetaV3(byte[] encodeMeta) {
    ByteBuffer buffer = ByteBuffer.wrap(encodeMeta);
    char measureType = buffer.getChar();
    ValueEncoderMeta valueEncoderMeta = new ValueEncoderMeta();
    valueEncoderMeta.setType(measureType);
    switch (measureType) {
      case DataType.DOUBLE_MEASURE_CHAR:
        valueEncoderMeta.setMaxValue(buffer.getDouble());
        valueEncoderMeta.setMinValue(buffer.getDouble());
        valueEncoderMeta.setUniqueValue(buffer.getDouble());
        break;
      case DataType.BIG_DECIMAL_MEASURE_CHAR:
        valueEncoderMeta.setMaxValue(BigDecimal.valueOf(Long.MAX_VALUE));
        valueEncoderMeta.setMinValue(BigDecimal.valueOf(Long.MIN_VALUE));
        valueEncoderMeta.setUniqueValue(BigDecimal.valueOf(Long.MIN_VALUE));
        break;
      case DataType.BIG_INT_MEASURE_CHAR:
        valueEncoderMeta.setMaxValue(buffer.getLong());
        valueEncoderMeta.setMinValue(buffer.getLong());
        valueEncoderMeta.setUniqueValue(buffer.getLong());
        break;
      default:
        throw new IllegalArgumentException("invalid measure type: " + measureType);
    }
    valueEncoderMeta.setDecimal(buffer.getInt());
    valueEncoderMeta.setDataTypeSelected(buffer.get());
    return valueEncoderMeta;
  }

  /**
   * Below method will be used to convert indexes in range
   * Indexes=[0,1,2,3,4,5,6,7,8,9]
   * Length=9
   * number of element in group =5
   * then output will be [0,1,2,3,4],[5,6,7,8],[9]
   *
   * @param indexes                indexes
   * @param length                 number of element to be considered
   * @param numberOfElementInGroup number of element in group
   * @return range indexes
   */
  public static int[][] getRangeIndex(int[] indexes, int length, int numberOfElementInGroup) {
    List<List<Integer>> rangeList = new ArrayList<>();
    int[][] outputArray = null;
    int k = 0;
    int index = 1;
    if (indexes.length == 1) {
      outputArray = new int[1][2];
      outputArray[0][0] = indexes[0];
      outputArray[0][1] = indexes[0];
      return outputArray;
    }
    while (index < length) {
      if (indexes[index] - indexes[index - 1] == 1 && k < numberOfElementInGroup - 1) {
        k++;
      } else {
        if (k > 0) {
          List<Integer> range = new ArrayList<>();
          rangeList.add(range);
          range.add(indexes[index - k - 1]);
          range.add(indexes[index - 1]);
        } else {
          List<Integer> range = new ArrayList<>();
          rangeList.add(range);
          range.add(indexes[index - 1]);
        }
        k = 0;
      }
      index++;
    }
    if (k > 0) {
      List<Integer> range = new ArrayList<>();
      rangeList.add(range);
      range.add(indexes[index - k - 1]);
      range.add(indexes[index - 1]);
    } else {
      List<Integer> range = new ArrayList<>();
      rangeList.add(range);
      range.add(indexes[index - 1]);

    }
    if (length != indexes.length) {
      List<Integer> range = new ArrayList<>();
      rangeList.add(range);
      range.add(indexes[indexes.length - 1]);
    }

    // as diving in range so array size will be always 2
    outputArray = new int[rangeList.size()][2];
    for (int i = 0; i < outputArray.length; i++) {
      if (rangeList.get(i).size() == 1) {
        outputArray[i][0] = rangeList.get(i).get(0);
        outputArray[i][1] = rangeList.get(i).get(0);
      } else {
        outputArray[i][0] = rangeList.get(i).get(0);
        outputArray[i][1] = rangeList.get(i).get(1);
      }
    }
    return outputArray;
  }

  public static void freeMemory(DimensionRawColumnChunk[] dimensionRawColumnChunks,
      MeasureRawColumnChunk[] measureRawColumnChunks) {
    if (null != measureRawColumnChunks) {
      for (MeasureRawColumnChunk measureRawColumnChunk : measureRawColumnChunks) {
        if (null != measureRawColumnChunk) {
          measureRawColumnChunk.freeMemory();
        }
      }
    }
    if (null != dimensionRawColumnChunks) {
      for (DimensionRawColumnChunk dimensionRawColumnChunk : dimensionRawColumnChunks) {
        if (null != dimensionRawColumnChunk) {
          dimensionRawColumnChunk.freeMemory();
        }
      }
    }
  }

  /**
   * @param invalidBlockVOForSegmentId
   * @param updateStatusManager
   * @return
   */
  public static boolean isInvalidTableBlock(String segmentId, String filePath,
      UpdateVO invalidBlockVOForSegmentId, SegmentUpdateStatusManager updateStatusManager) {

    if (!updateStatusManager.isBlockValid(segmentId,
        CarbonTablePath.getCarbonDataFileName(filePath) +
            CarbonTablePath.getCarbonDataExtension())) {
      return true;
    }
    // in case of compaction over si table the factTimeStamp will be null for the
    // main table's compacted segments in that case no need to validate the block
    if (null != invalidBlockVOForSegmentId &&
        null != invalidBlockVOForSegmentId.getFactTimestamp()) {
      long blockTimeStamp =
          Long.parseLong(CarbonTablePath.DataFileUtil.getTimeStampFromFileName(filePath));
      if ((blockTimeStamp > invalidBlockVOForSegmentId.getFactTimestamp() && (
          invalidBlockVOForSegmentId.getUpdateDeltaStartTimestamp() != null
              && blockTimeStamp < invalidBlockVOForSegmentId.getUpdateDeltaStartTimestamp()))) {
        return true;
      }
      // aborted files case.
      if (invalidBlockVOForSegmentId.getLatestUpdateTimestamp() != null
          && blockTimeStamp > invalidBlockVOForSegmentId.getLatestUpdateTimestamp()) {
        return true;
      }
      // for 1st time, start timestamp will be empty, so need to consider fact timestamp.
      if (null == invalidBlockVOForSegmentId.getUpdateDeltaStartTimestamp()
          && blockTimeStamp > invalidBlockVOForSegmentId.getFactTimestamp()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Below method will be used to get the format for
   * date or timestamp data type from property. This
   * is added to avoid the code duplication
   *
   * @param dataType
   * @return format
   */
  public static String getFormatFromProperty(DataType dataType) {
    if (dataType.equals(DataTypes.DATE)) {
      return CarbonProperties.getInstance().getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
          CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT);
    } else if (dataType.equals(DataTypes.TIMESTAMP)) {
      return CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
              CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
    } else {
      return null;
    }
  }

  /**
   * Get formatted date/timestamp string
   * @param format Expected format
   * @param dataType Data type(date or timestamp)
   * @param value value
   * @param isLiteral isLiteral
   * @return
   */
  public static String getFormattedDateOrTimestamp(String format, DataType dataType, Object value,
      boolean isLiteral) {
    SimpleDateFormat parser = new SimpleDateFormat(format);
    if (dataType == DataTypes.DATE) {
      parser.setTimeZone(TimeZone.getTimeZone("GMT"));
    }
    if (value instanceof Timestamp) {
      return parser.format((Timestamp) value);
    } else if (value instanceof java.sql.Date) {
      return parser.format((java.sql.Date) value);
    } else if (value instanceof Long) {
      if (isLiteral) {
        return parser.format(new Timestamp((long) value / 1000));
      }
      return parser.format(new Timestamp((long) value));
    } else if (value instanceof Integer) {
      long date = ((int) value) * DateDirectDictionaryGenerator.MILLIS_PER_DAY;
      return parser.format(new java.sql.Date(date));
    }
    return value.toString();
  }

  /**
   * Below method will be used to convert byte data to surrogate key based
   * column value size
   *
   * @param data                data
   * @param startOffsetOfData   start offset of data
   * @param eachColumnValueSize size of each column value
   * @return surrogate key
   */
  public static int getSurrogateInternal(byte[] data, int startOffsetOfData,
      int eachColumnValueSize) {
    int surrogate = 0;
    switch (eachColumnValueSize) {
      case 1:
        surrogate <<= 8;
        surrogate ^= data[startOffsetOfData] & 0xFF;
        return surrogate;
      case 2:
        surrogate <<= 8;
        surrogate ^= data[startOffsetOfData] & 0xFF;
        surrogate <<= 8;
        surrogate ^= data[startOffsetOfData + 1] & 0xFF;
        return surrogate;
      case 3:
        surrogate <<= 8;
        surrogate ^= data[startOffsetOfData] & 0xFF;
        surrogate <<= 8;
        surrogate ^= data[startOffsetOfData + 1] & 0xFF;
        surrogate <<= 8;
        surrogate ^= data[startOffsetOfData + 2] & 0xFF;
        return surrogate;
      case 4:
        surrogate <<= 8;
        surrogate ^= data[startOffsetOfData] & 0xFF;
        surrogate <<= 8;
        surrogate ^= data[startOffsetOfData + 1] & 0xFF;
        surrogate <<= 8;
        surrogate ^= data[startOffsetOfData + 2] & 0xFF;
        surrogate <<= 8;
        surrogate ^= data[startOffsetOfData + 3] & 0xFF;
        return surrogate;
      default:
        throw new IllegalArgumentException("Int cannot be more than 4 bytes: " +
            eachColumnValueSize);
    }
  }

  /**
   * Validate boolean value configuration
   *
   * @param value
   * @return
   */
  public static boolean validateBoolean(String value) {
    if (null == value) {
      return false;
    } else if (!("false".equalsIgnoreCase(value) || "true".equalsIgnoreCase(value))) {
      return false;
    }
    return true;
  }

  /**
   * validate the sort scope
   * @param sortScope
   * @return
   */
  public static boolean isValidSortOption(String sortScope) {
    if (sortScope == null) {
      return false;
    }
    switch (sortScope.toUpperCase()) {
      case "LOCAL_SORT":
      case "NO_SORT":
      case "GLOBAL_SORT":
        return true;
      default:
        return false;
    }
  }

  /**
   * validate the storage level
   * @param storageLevel
   * @return boolean
   */
  public static boolean isValidStorageLevel(String storageLevel) {
    if (null == storageLevel || storageLevel.trim().equals("")) {
      return false;
    }
    switch (storageLevel.toUpperCase()) {
      case "DISK_ONLY":
      case "DISK_ONLY_2":
      case "MEMORY_ONLY":
      case "MEMORY_ONLY_2":
      case "MEMORY_ONLY_SER":
      case "MEMORY_ONLY_SER_2":
      case "MEMORY_AND_DISK":
      case "MEMORY_AND_DISK_2":
      case "MEMORY_AND_DISK_SER":
      case "MEMORY_AND_DISK_SER_2":
      case "OFF_HEAP":
      case "NONE":
        return true;
      default:
        return false;
    }
  }

  /**
   * validate teh batch size
   *
   * @param value
   * @return
   */
  public static boolean validateValidIntType(String value) {
    if (null == value) {
      return false;
    }
    try {
      Integer.parseInt(value);
    } catch (NumberFormatException nfe) {
      return false;
    }
    return true;
  }

  /**
   * is valid store path
   * @param badRecordsLocation
   * @return
   */
  public static boolean isValidBadStorePath(String badRecordsLocation) {
    if (StringUtils.isEmpty(badRecordsLocation)) {
      return false;
    } else {
      return isFileExists(checkAndAppendHDFSUrl(badRecordsLocation));
    }
  }

  /**
   * Converts TableInfo object to json multi string objects of size 4000
   * @param tableInfo
   * @param separator separator between each string
   * @param quote Quote to be used for string
   * @param prefix Prefix to be added before generated string
   * @return
   */
  public static String convertToMultiGsonStrings(TableInfo tableInfo, String separator,
      String quote, String prefix) {
    Gson gson = new Gson();
    String schemaString = gson.toJson(tableInfo);
    return splitSchemaStringToMultiString(separator, quote, prefix, schemaString);
  }

  /**
   * Converts Json String to multi string objects of size 4000
   * @param schemaString Json string
   * @param separator separator between each string
   * @param quote Quote to be used for string
   * @param prefix Prefix to be added before generated string
   * @return
   */
  public static String splitSchemaStringToMultiString(String separator, String quote,
      String prefix, String schemaString) {
    int schemaLen = schemaString.length();
    int splitLen = 4000;
    int parts = schemaLen / splitLen;
    if (schemaLen % splitLen > 0) {
      parts++;
    }
    StringBuilder builder =
        new StringBuilder(prefix).append(quote).append("carbonSchemaPartsNo").append(quote)
            .append(separator).append("'").append(parts).append("',");
    int runningLen = 0;
    int endLen = schemaLen > splitLen ? splitLen : schemaLen;
    for (int i = 0; i < parts; i++) {
      if (i == parts - 1) {
        if (schemaLen % splitLen > 0) {
          endLen = schemaLen % splitLen;
        }
      }
      builder.append(quote).append("carbonSchema").append(i).append(quote).append(separator);
      builder.append("'").append(schemaString, runningLen, runningLen + endLen)
          .append("'");
      if (i < parts - 1) {
        builder.append(",");
      }
      runningLen += splitLen;
    }
    return builder.toString();
  }

  /**
   * Converts TableInfo object to json multi string objects  of size 4000 and stored in map
   * @param tableInfo
   * @return
   */
  public static Map<String, String> convertToMultiStringMap(TableInfo tableInfo) {
    Gson gson = new Gson();
    String schemaString = gson.toJson(tableInfo);
    return splitSchemaStringToMap(schemaString);
  }

  /**
   * Converts Json string to multi string objects  of size 4000 and stored in map
   *
   * @param schemaString
   * @return
   */
  public static Map<String, String> splitSchemaStringToMap(String schemaString) {
    Map<String, String> map = new HashMap<>();
    int schemaLen = schemaString.length();
    int splitLen = 4000;
    int parts = schemaLen / splitLen;
    if (schemaLen % splitLen > 0) {
      parts++;
    }
    map.put("carbonSchemaPartsNo", parts + "");
    int runningLen = 0;
    int endLen = schemaLen > splitLen ? splitLen : schemaLen;
    for (int i = 0; i < parts; i++) {
      if (i == parts - 1) {
        if (schemaLen % splitLen > 0) {
          endLen = schemaLen % splitLen;
        }
      }
      map.put("carbonSchema" + i, schemaString.substring(runningLen, runningLen + endLen));
      runningLen += splitLen;
    }
    return map;
  }

  // TODO: move this to carbon store API as it is related to TableInfo creation
  public static TableInfo convertGsonToTableInfo(Map<String, String> properties) {
    String partsNo = properties.get("carbonSchemaPartsNo");
    if (partsNo == null) {
      return null;
    }
    int no = Integer.parseInt(partsNo);
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < no; i++) {
      String part = properties.get("carbonSchema" + i);
      if (part == null) {
        throw new RuntimeException("Some thing wrong in getting schema from hive metastore");
      }
      builder.append(part);
    }

    // Datatype GSON adapter is added to support backward compatibility for tableInfo
    // deserialization
    GsonBuilder gsonBuilder = new GsonBuilder();
    gsonBuilder.registerTypeAdapter(DataType.class, new DataTypeAdapter());

    Gson gson = gsonBuilder.create();
    // if the column name contains backslash in the column name, then fromJson will remove that,
    // so replace like below to keep the "\" in column name and write the proper name in the schema
    TableInfo tableInfo = gson.fromJson(builder.toString().replace("\\", "\\\\"), TableInfo.class);

    // The tableInfo is deserialize from GSON string, need to update the scale and
    // precision if there are any decimal field, because DecimalType is added in Carbon 1.3,
    // If it is not updated, read compatibility will be break for table generated before Carbon 1.3
    updateDecimalType(tableInfo);
    return tableInfo;
  }

  // Update decimal type inside `tableInfo` to set scale and precision, if there are any decimal
  private static void updateDecimalType(TableInfo tableInfo) {
    List<ColumnSchema> deserializeColumns = tableInfo.getFactTable().getListOfColumns();
    for (ColumnSchema column : deserializeColumns) {
      DataType dataType = column.getDataType();
      if (DataTypes.isDecimal(dataType)) {
        column.setDataType(DataTypes.createDecimalType(column.getPrecision(), column.getScale()));
      }
    }
  }

  /**
   * Removes schema from properties
   * @param properties
   * @return
   */
  public static Map<String, String> removeSchemaFromMap(Map<String, String> properties) {
    Map<String, String> newMap = new HashMap<>(properties);
    String partsNo = newMap.get("carbonSchemaPartsNo");
    if (partsNo == null) {
      return newMap;
    }
    int no = Integer.parseInt(partsNo);
    for (int i = 0; i < no; i++) {
      newMap.remove("carbonSchema" + i);
    }
    return newMap;
  }

  /**
   * This method will read the schema file from a given path
   *
   * @param schemaFilePath
   * @return
   */
  public static org.apache.carbondata.format.TableInfo readSchemaFile(String schemaFilePath)
      throws IOException {
    return readSchemaFile(schemaFilePath, FileFactory.getConfiguration());
  }

  public static org.apache.carbondata.format.TableInfo readSchemaFile(String schemaFilePath,
      Configuration conf)
      throws IOException {
    TBaseCreator createTBase = org.apache.carbondata.format.TableInfo::new;
    ThriftReader thriftReader = new ThriftReader(schemaFilePath, createTBase, conf);
    thriftReader.open();
    org.apache.carbondata.format.TableInfo tableInfo =
        (org.apache.carbondata.format.TableInfo) thriftReader.read();
    thriftReader.close();
    return tableInfo;
  }

  public static ColumnSchema thriftColumnSchemaToWrapperColumnSchema(
      org.apache.carbondata.format.ColumnSchema externalColumnSchema) {
    ColumnSchema wrapperColumnSchema = new ColumnSchema();
    wrapperColumnSchema.setColumnUniqueId(externalColumnSchema.getColumn_id());
    wrapperColumnSchema.setColumnReferenceId(externalColumnSchema.getColumnReferenceId());
    wrapperColumnSchema.setColumnName(externalColumnSchema.getColumn_name());
    DataType dataType = thriftDataTypeToWrapperDataType(externalColumnSchema.data_type);
    if (DataTypes.isDecimal(dataType)) {
      DecimalType decimalType = (DecimalType) dataType;
      decimalType.setPrecision(externalColumnSchema.getPrecision());
      decimalType.setScale(externalColumnSchema.getScale());
    }
    wrapperColumnSchema.setDataType(dataType);
    wrapperColumnSchema.setDimensionColumn(externalColumnSchema.isDimension());
    List<Encoding> encoders = new ArrayList<Encoding>();
    for (org.apache.carbondata.format.Encoding encoder : externalColumnSchema.getEncoders()) {
      encoders.add(fromExternalToWrapperEncoding(encoder));
    }
    wrapperColumnSchema.setEncodingList(encoders);
    wrapperColumnSchema.setNumberOfChild(externalColumnSchema.getNum_child());
    wrapperColumnSchema.setPrecision(externalColumnSchema.getPrecision());
    wrapperColumnSchema.setScale(externalColumnSchema.getScale());
    wrapperColumnSchema.setDefaultValue(externalColumnSchema.getDefault_value());
    wrapperColumnSchema.setSchemaOrdinal(externalColumnSchema.getSchemaOrdinal());
    Map<String, String> properties = externalColumnSchema.getColumnProperties();
    if (properties != null) {
      if (properties.get(CarbonCommonConstants.SORT_COLUMNS) != null) {
        wrapperColumnSchema.setSortColumn(true);
      }
    }
    wrapperColumnSchema.setColumnProperties(properties);
    wrapperColumnSchema.setFunction(externalColumnSchema.getAggregate_function());
    List<org.apache.carbondata.format.ParentColumnTableRelation> parentColumnTableRelation =
        externalColumnSchema.getParentColumnTableRelations();
    if (null != parentColumnTableRelation) {
      wrapperColumnSchema.setParentColumnTableRelations(
          fromThriftToWrapperParentTableColumnRelations(parentColumnTableRelation));
    }
    return wrapperColumnSchema;
  }

  static List<ParentColumnTableRelation> fromThriftToWrapperParentTableColumnRelations(
      List<org.apache.carbondata.format.ParentColumnTableRelation> thriftParentColumnRelation) {
    List<ParentColumnTableRelation> parentColumnTableRelationList = new ArrayList<>();
    for (org.apache.carbondata.format.ParentColumnTableRelation carbonTableRelation :
        thriftParentColumnRelation) {
      RelationIdentifier relationIdentifier =
          new RelationIdentifier(carbonTableRelation.getRelationIdentifier().getDatabaseName(),
              carbonTableRelation.getRelationIdentifier().getTableName(),
              carbonTableRelation.getRelationIdentifier().getTableId());
      ParentColumnTableRelation parentColumnTableRelation =
          new ParentColumnTableRelation(relationIdentifier, carbonTableRelation.getColumnId(),
              carbonTableRelation.getColumnName());
      parentColumnTableRelationList.add(parentColumnTableRelation);
    }
    return parentColumnTableRelationList;
  }

  static Encoding fromExternalToWrapperEncoding(
      org.apache.carbondata.format.Encoding encoderThrift) {
    switch (encoderThrift) {
      case DICTIONARY:
        return Encoding.DICTIONARY;
      case DELTA:
        return Encoding.DELTA;
      case RLE:
        return Encoding.RLE;
      case INVERTED_INDEX:
        return Encoding.INVERTED_INDEX;
      case BIT_PACKED:
        return Encoding.BIT_PACKED;
      case DIRECT_DICTIONARY:
        return Encoding.DIRECT_DICTIONARY;
      default:
        throw new IllegalArgumentException(encoderThrift.toString() + " is not supported");
    }
  }

  static DataType thriftDataTypeToWrapperDataType(
      org.apache.carbondata.format.DataType dataTypeThrift) {
    switch (dataTypeThrift) {
      case BOOLEAN:
        return DataTypes.BOOLEAN;
      case STRING:
        return DataTypes.STRING;
      case SHORT:
        return DataTypes.SHORT;
      case INT:
        return DataTypes.INT;
      case LONG:
        return DataTypes.LONG;
      case DOUBLE:
        return DataTypes.DOUBLE;
      case DECIMAL:
        return DataTypes.createDefaultDecimalType();
      case DATE:
        return DataTypes.DATE;
      case TIMESTAMP:
        return DataTypes.TIMESTAMP;
      case ARRAY:
        return DataTypes.createDefaultArrayType();
      case STRUCT:
        return DataTypes.createDefaultStructType();
      case MAP:
        return DataTypes.createDefaultMapType();
      case VARCHAR:
        return DataTypes.VARCHAR;
      case FLOAT:
        return DataTypes.FLOAT;
      case BYTE:
        return DataTypes.BYTE;
      case BINARY:
        return DataTypes.BINARY;
      default:
        LOGGER.warn(String.format("Cannot match the data type, using default String data type: %s",
            DataTypes.STRING.getName()));
        return DataTypes.STRING;
    }
  }

  public static String getFilePathExternalFilePath(String path, Configuration configuration) {

    // return the list of carbondata files in the given path.
    CarbonFile segment = FileFactory.getCarbonFile(path, configuration);

    CarbonFile[] dataFiles = segment.listFiles();
    CarbonFile latestCarbonFile = null;
    long latestDatafileTimestamp = 0L;
    // get the latest carbon data file to get the latest schema in the folder
    for (CarbonFile dataFile : dataFiles) {
      if (dataFile.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT)
          && dataFile.getLastModifiedTime() > latestDatafileTimestamp) {
        latestCarbonFile = dataFile;
        latestDatafileTimestamp = dataFile.getLastModifiedTime();
      } else if (dataFile.isDirectory()) {
        // if the list has directories that doesn't contain data files,
        // continue checking other files/directories in the list.
        if (getFilePathExternalFilePath(dataFile.getAbsolutePath(), configuration) == null) {
          continue;
        } else {
          return getFilePathExternalFilePath(dataFile.getAbsolutePath(), configuration);
        }
      }
    }

    if (latestCarbonFile != null) {
      return latestCarbonFile.getAbsolutePath();
    } else {
      //returning null only if the path doesn't have data files.
      return null;
    }
  }

  /**
   * This method will read the schema file from a given path
   *
   * @return table info containing the schema
   */
  public static org.apache.carbondata.format.TableInfo inferSchema(String carbonDataFilePath,
      String tableName, boolean isCarbonFileProvider, Configuration configuration)
      throws IOException {
    String fistFilePath = null;
    if (isCarbonFileProvider) {
      fistFilePath = getFilePathExternalFilePath(carbonDataFilePath + "/Fact/Part0/Segment_null",
          configuration);
    } else {
      fistFilePath = getFilePathExternalFilePath(carbonDataFilePath, configuration);
    }
    if (fistFilePath == null) {
      // Check if we can infer the schema from the hive metastore.
      LOGGER.error("CarbonData file is not present in the table location");
      throw new FileNotFoundException("CarbonData file is not present in the table location");
    }
    CarbonHeaderReader carbonHeaderReader = new CarbonHeaderReader(fistFilePath, configuration);
    List<ColumnSchema> columnSchemaList = carbonHeaderReader.readSchema();
    // only columnSchema is the valid entry, reset all dummy entries.
    TableSchema tableSchema = getDummyTableSchema(tableName, columnSchemaList);

    ThriftWrapperSchemaConverterImpl thriftWrapperSchemaConverter =
        new ThriftWrapperSchemaConverterImpl();
    org.apache.carbondata.format.TableSchema thriftFactTable =
        thriftWrapperSchemaConverter.fromWrapperToExternalTableSchema(tableSchema);
    return new org.apache.carbondata.format.TableInfo(thriftFactTable,
        new ArrayList<org.apache.carbondata.format.TableSchema>());
  }

  /**
   * This method will prepare dummy tableInfo
   *
   * @param carbonDataFilePath
   * @param tableName
   * @return
   */
  public static TableInfo buildDummyTableInfo(String carbonDataFilePath,
      String tableName, String dbName) {
    // During SDK carbon Reader, This method will be called.
    // This API will avoid IO operation to get the columnSchema list.
    // ColumnSchema list will be filled during blocklet loading (where actual IO happens)
    List<ColumnSchema> columnSchemaList = new ArrayList<>();
    TableSchema tableSchema = getDummyTableSchema(tableName, columnSchemaList);
    ThriftWrapperSchemaConverterImpl thriftWrapperSchemaConverter =
        new ThriftWrapperSchemaConverterImpl();
    org.apache.carbondata.format.TableSchema thriftFactTable =
        thriftWrapperSchemaConverter.fromWrapperToExternalTableSchema(tableSchema);
    org.apache.carbondata.format.TableInfo tableInfo =
        new org.apache.carbondata.format.TableInfo(thriftFactTable,
            new ArrayList<org.apache.carbondata.format.TableSchema>());
    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    TableInfo wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(
        tableInfo, dbName, tableName, carbonDataFilePath);
    wrapperTableInfo.setTransactionalTable(false);
    return wrapperTableInfo;
  }

  /**
   * This method will infer the schema file from a given index file path
   * @param indexFilePath
   * @param tableName
   * @return
   * @throws IOException
   */
  public static org.apache.carbondata.format.TableInfo inferSchemaFromIndexFile(
      String indexFilePath, String tableName) throws IOException {
    CarbonIndexFileReader indexFileReader = new CarbonIndexFileReader();
    try {
      indexFileReader.openThriftReader(indexFilePath);
      org.apache.carbondata.format.IndexHeader readIndexHeader = indexFileReader.readIndexHeader();
      List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
      List<org.apache.carbondata.format.ColumnSchema> table_columns =
          readIndexHeader.getTable_columns();
      for (int i = 0; i < table_columns.size(); i++) {
        columnSchemaList.add(thriftColumnSchemaToWrapperColumnSchema(table_columns.get(i)));
      }
      // only columnSchema is the valid entry, reset all dummy entries.
      TableSchema tableSchema = getDummyTableSchema(tableName, columnSchemaList);

      ThriftWrapperSchemaConverterImpl thriftWrapperSchemaConverter =
          new ThriftWrapperSchemaConverterImpl();
      org.apache.carbondata.format.TableSchema thriftFactTable =
          thriftWrapperSchemaConverter.fromWrapperToExternalTableSchema(tableSchema);

      return new org.apache.carbondata.format.TableInfo(thriftFactTable,
          new ArrayList<org.apache.carbondata.format.TableSchema>());
    } finally {
      indexFileReader.closeThriftReader();
    }
  }

  private static TableSchema getDummyTableSchema(String tableName,
      List<ColumnSchema> columnSchemaList) {
    TableSchema tableSchema = new TableSchema();
    tableSchema.setTableName(tableName);
    tableSchema.setBucketingInfo(null);
    tableSchema.setSchemaEvolution(null);
    tableSchema.setTableId(UUID.randomUUID().toString());
    tableSchema.setListOfColumns(columnSchemaList);

    SchemaEvolutionEntry schemaEvolutionEntry = new SchemaEvolutionEntry();
    schemaEvolutionEntry.setTimeStamp(System.currentTimeMillis());
    SchemaEvolution schemaEvolution = new SchemaEvolution();
    List<SchemaEvolutionEntry> schEntryList = new ArrayList<>();
    schEntryList.add(schemaEvolutionEntry);
    schemaEvolution.setSchemaEvolutionEntryList(schEntryList);
    tableSchema.setSchemaEvolution(schemaEvolution);
    return tableSchema;
  }

  public static void dropDatabaseDirectory(String databasePath)
      throws IOException, InterruptedException {
    if (FileFactory.isFileExist(databasePath)) {
      CarbonFile dbPath = FileFactory.getCarbonFile(databasePath);
      CarbonUtil.deleteFoldersAndFiles(dbPath);
    }
  }

  /**
   * convert value to byte array
   */
  public static byte[] getValueAsBytes(DataType dataType, Object value) {
    ByteBuffer b;
    if (dataType == DataTypes.BYTE || dataType == DataTypes.BOOLEAN) {
      byte[] bytes = new byte[1];
      bytes[0] = (byte) value;
      return bytes;
    } else if (dataType == DataTypes.SHORT) {
      b = ByteBuffer.allocate(8);
      b.putLong((short) value);
      b.flip();
      return b.array();
    } else if (dataType == DataTypes.INT) {
      b = ByteBuffer.allocate(8);
      b.putLong((int) value);
      b.flip();
      return b.array();
    } else if (dataType == DataTypes.LONG || dataType == DataTypes.TIMESTAMP) {
      b = ByteBuffer.allocate(8);
      b.putLong((long) value);
      b.flip();
      return b.array();
    } else if (dataType == DataTypes.DOUBLE) {
      b = ByteBuffer.allocate(8);
      b.putDouble((double) value);
      b.flip();
      return b.array();
    } else if (dataType == DataTypes.FLOAT) {
      b = ByteBuffer.allocate(8);
      b.putFloat((float) value);
      b.flip();
      return b.array();
    } else if (DataTypes.isDecimal(dataType)) {
      return DataTypeUtil.bigDecimalToByte((BigDecimal) value);
    } else if (dataType == DataTypes.BYTE_ARRAY || dataType == DataTypes.BINARY
        || dataType == DataTypes.STRING
        || dataType == DataTypes.DATE
        || dataType == DataTypes.VARCHAR) {
      return (byte[]) value;
    } else {
      throw new IllegalArgumentException("Invalid data type: " + dataType);
    }
  }

  public static boolean validateRangeOfSegmentList(String segmentId)
      throws InvalidConfigurationException {
    String[] values = segmentId.split(",");
    try {
      if (values.length == 0) {
        throw new InvalidConfigurationException(
            "carbon.input.segments.<database_name>.<table_name> value can't be empty.");
      }
      for (String value : values) {
        if (!value.equalsIgnoreCase("*")) {
          Segment segment = Segment.toSegment(value, null);
          float aFloatValue = Float.parseFloat(segment.getSegmentNo());
          if (aFloatValue < 0 || aFloatValue > Float.MAX_VALUE) {
            throw new InvalidConfigurationException(
                "carbon.input.segments.<database_name>.<table_name> value range should be greater "
                    + "than 0 and less than " + Float.MAX_VALUE);
          }
        }
      }
    } catch (NumberFormatException nfe) {
      throw new InvalidConfigurationException(
          "carbon.input.segments.<database_name>.<table_name> value range is not valid");
    }
    return true;
  }

  /**
   * Below method will be used to check whether bitset applied on previous filter
   * can be used to apply on next column filter
   * @param usePrvBitSetGroup
   * @param prvBitsetGroup
   * @param pageNumber
   * @param numberOfFilterValues
   * @return
   */
  public static boolean usePreviousFilterBitsetGroup(boolean usePrvBitSetGroup,
      BitSetGroup prvBitsetGroup, int pageNumber, int numberOfFilterValues) {
    if (!usePrvBitSetGroup || null == prvBitsetGroup || null == prvBitsetGroup.getBitSet(pageNumber)
        || prvBitsetGroup.getBitSet(pageNumber).isEmpty()) {
      return false;
    }
    int numberOfRowSelected = prvBitsetGroup.getBitSet(pageNumber).cardinality();
    return numberOfFilterValues > numberOfRowSelected;
  }

  /**
   * Below method will be used to check filter value is present in the data chunk or not
   * @param filterValues
   * @param dimensionColumnPage
   * @param low
   * @param high
   * @param chunkRowIndex
   * @return
   */
  public static int isFilterPresent(byte[][] filterValues,
      DimensionColumnPage dimensionColumnPage, int low, int high, int chunkRowIndex) {
    int compareResult = 0;
    int mid = 0;
    while (low <= high) {
      mid = (low + high) >>> 1;
      compareResult = dimensionColumnPage.compareTo(chunkRowIndex, filterValues[mid]);
      if (compareResult < 0) {
        high = mid - 1;
      } else if (compareResult > 0) {
        low = mid + 1;
      } else {
        return compareResult;
      }
    }
    return -1;
  }

  /**
   * This method will calculate the data size and index size for carbon table
   */
  public static Map<String, Long> calculateDataIndexSize(CarbonTable carbonTable,
      Boolean updateSize)
      throws IOException {
    Map<String, Long> dataIndexSizeMap = new HashMap<String, Long>();
    long totalDataSize = 0L;
    long totalIndexSize = 0L;
    long lastUpdateTime = 0L;
    boolean needUpdate = false;
    AbsoluteTableIdentifier identifier = carbonTable.getAbsoluteTableIdentifier();
    String isCalculated = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.ENABLE_CALCULATE_SIZE,
            CarbonCommonConstants.DEFAULT_ENABLE_CALCULATE_SIZE);
    if (isCalculated.equalsIgnoreCase("true")) {
      SegmentStatusManager segmentStatusManager = new SegmentStatusManager(identifier);
      ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();
      try {
        boolean lockAcquired = true;
        if (updateSize) {
          lockAcquired = carbonLock.lockWithRetries();
        }
        if (lockAcquired) {
          LOGGER.debug("Acquired lock for table for table status update");
          String metadataPath = carbonTable.getMetadataPath();
          LoadMetadataDetails[] loadMetadataDetails =
              SegmentStatusManager.readLoadMetadata(metadataPath);

          for (LoadMetadataDetails loadMetadataDetail : loadMetadataDetails) {
            SegmentStatus loadStatus = loadMetadataDetail.getSegmentStatus();
            if (loadStatus == SegmentStatus.SUCCESS || loadStatus ==
                      SegmentStatus.LOAD_PARTIAL_SUCCESS) {
              String dataSize = loadMetadataDetail.getDataSize();
              String indexSize = loadMetadataDetail.getIndexSize();
              // If it is old segment, need to calculate data size and index size again
              if (null == dataSize || null == indexSize) {
                needUpdate = true;
                LOGGER.debug("It is an old segment, need calculate data size and index size again");
                HashMap<String, Long> map = CarbonUtil.getDataSizeAndIndexSize(
                    identifier.getTablePath(), loadMetadataDetail.getLoadName());
                dataSize = String.valueOf(map.get(CarbonCommonConstants.CARBON_TOTAL_DATA_SIZE));
                indexSize = String.valueOf(map.get(CarbonCommonConstants.CARBON_TOTAL_INDEX_SIZE));
                loadMetadataDetail.setDataSize(dataSize);
                loadMetadataDetail.setIndexSize(indexSize);
              }
              totalDataSize += Long.parseLong(dataSize);
              totalIndexSize += Long.parseLong(indexSize);
            }
          }
          // If it contains old segment, write new load details
          if (needUpdate && updateSize) {
            SegmentStatusManager.writeLoadDetailsIntoFile(
                CarbonTablePath.getTableStatusFilePath(identifier.getTablePath()),
                loadMetadataDetails);
          }
          String tableStatusPath =
              CarbonTablePath.getTableStatusFilePath(identifier.getTablePath());
          if (FileFactory.isFileExist(tableStatusPath)) {
            lastUpdateTime =
                FileFactory.getCarbonFile(tableStatusPath).getLastModifiedTime();
          }
          if (!FileFactory.isFileExist(metadataPath)) {
            totalDataSize = FileFactory.getDirectorySize(carbonTable.getTablePath());
          }
          dataIndexSizeMap.put(CarbonCommonConstants.CARBON_TOTAL_DATA_SIZE, totalDataSize);
          dataIndexSizeMap.put(CarbonCommonConstants.CARBON_TOTAL_INDEX_SIZE, totalIndexSize);
          dataIndexSizeMap.put(CarbonCommonConstants.LAST_UPDATE_TIME, lastUpdateTime);
        } else {
          LOGGER.error("Not able to acquire the lock for Table status update for table");
        }
      } finally {
        if (updateSize) {
          if (carbonLock.unlock()) {
            LOGGER.debug("Table unlocked successfully after table status update");
          } else {
            LOGGER.error("Unable to unlock Table lock for table during table status update");
          }
        }
      }
    }
    return dataIndexSizeMap;
  }

  // Get the total size of carbon data and the total size of carbon index
  public static HashMap<String, Long> getDataSizeAndIndexSize(
      String segmentPath) throws IOException {
    if (segmentPath == null) {
      throw new IllegalArgumentException("Argument [segmentPath] is null.");
    }
    long carbonDataSize = 0L;
    long carbonIndexSize = 0L;
    HashMap<String, Long> dataAndIndexSize = new HashMap<String, Long>();
    FileFactory.FileType fileType = FileFactory.getFileType(segmentPath);
    switch (fileType) {
      case HDFS:
      case ALLUXIO:
      case VIEWFS:
      case S3:
      case CUSTOM:
        Path path = new Path(segmentPath);
        FileSystem fs = path.getFileSystem(FileFactory.getConfiguration());
        if (fs.exists(path)) {
          FileStatus[] fileStatuses = fs.listStatus(path);
          if (null != fileStatuses) {
            FileStatus[] validDataAndIndexStatus =
                (FileStatus[]) SegmentFileStore.getValidCarbonIndexFiles(fileStatuses);
            for (FileStatus dataAndIndexStatus : validDataAndIndexStatus) {
              String pathName = dataAndIndexStatus.getPath().getName();
              if (pathName.endsWith(CarbonTablePath.getCarbonIndexExtension()) || pathName
                  .endsWith(CarbonTablePath.getCarbonMergeIndexExtension())) {
                carbonIndexSize += dataAndIndexStatus.getLen();
              } else if (pathName.endsWith(CarbonTablePath.getCarbonDataExtension())) {
                carbonDataSize += dataAndIndexStatus.getLen();
              }
            }
          }
        }
        break;
      case LOCAL:
      default:
        segmentPath = FileFactory.getUpdatedFilePath(segmentPath);
        CarbonFile[] dataAndIndexFiles = FileFactory.getCarbonFile(segmentPath).listFiles();
        if (null != dataAndIndexFiles) {
          CarbonFile[] validDataAndIndexFiles =
              (CarbonFile[]) SegmentFileStore.getValidCarbonIndexFiles(dataAndIndexFiles);
          for (CarbonFile dataAndIndexFile : validDataAndIndexFiles) {
            if (dataAndIndexFile.getCanonicalPath()
                .endsWith(CarbonTablePath.getCarbonIndexExtension()) || dataAndIndexFile
                .getCanonicalPath().endsWith(CarbonTablePath.getCarbonMergeIndexExtension())) {
              carbonIndexSize += dataAndIndexFile.getSize();
            } else if (dataAndIndexFile.getCanonicalPath()
                .endsWith(CarbonTablePath.getCarbonDataExtension())) {
              carbonDataSize += dataAndIndexFile.getSize();
            }
          }
        }
    }
    dataAndIndexSize.put(CarbonCommonConstants.CARBON_TOTAL_DATA_SIZE, carbonDataSize);
    dataAndIndexSize.put(CarbonCommonConstants.CARBON_TOTAL_INDEX_SIZE, carbonIndexSize);
    return dataAndIndexSize;
  }

  // Get the total size of carbon data and the total size of carbon index
  private static HashMap<String, Long> getDataSizeAndIndexSize(String tablePath,
      String segmentId) throws IOException {
    return getDataSizeAndIndexSize(CarbonTablePath.getSegmentPath(tablePath, segmentId));
  }

  // Get the total size of carbon data and the total size of carbon index
  public static HashMap<String, Long> getDataSizeAndIndexSize(SegmentFileStore fileStore,
      boolean isCarbonSegment)
      throws IOException {
    long carbonDataSize = 0L;
    long carbonIndexSize = 0L;
    HashMap<String, Long> dataAndIndexSize = new HashMap<String, Long>();
    Map<String, SegmentFileStore.FolderDetails> locationMap = fileStore.getLocationMap();
    if (locationMap != null) {
      fileStore.readIndexFiles(FileFactory.getConfiguration());
      Map<String, List<String>> indexFilesMap = fileStore.getIndexFilesMap();
      // get the size of carbon index file
      carbonIndexSize = getCarbonIndexSize(fileStore, locationMap);
      for (Map.Entry<String, List<String>> entry : indexFilesMap.entrySet()) {
        // get the size of carbondata files
        for (String blockFile : entry.getValue()) {
          carbonDataSize += FileFactory.getCarbonFile(blockFile).getSize();
        }
      }
    }
    if (isCarbonSegment) {
      dataAndIndexSize.put(CarbonCommonConstants.CARBON_TOTAL_DATA_SIZE, carbonDataSize);
      dataAndIndexSize.put(CarbonCommonConstants.CARBON_TOTAL_INDEX_SIZE, carbonIndexSize);
    } else {
      // In case of non-carbon segments all the dataSize is collected in carbonIndexSize because of
      // listing behaviour of the SegmentFileStore.
      dataAndIndexSize.put(CarbonCommonConstants.CARBON_TOTAL_DATA_SIZE, carbonIndexSize);
      dataAndIndexSize.put(CarbonCommonConstants.CARBON_TOTAL_INDEX_SIZE, 0L);
    }
    return dataAndIndexSize;
  }

  /**
   * Calculate the index files size of the segment
   *
   * @param fileStore
   * @param locationMap
   * @return
   */
  public static long getCarbonIndexSize(SegmentFileStore fileStore,
      Map<String, SegmentFileStore.FolderDetails> locationMap) {
    long carbonIndexSize = 0L;
    for (Map.Entry<String, SegmentFileStore.FolderDetails> entry : locationMap.entrySet()) {
      SegmentFileStore.FolderDetails folderDetails = entry.getValue();
      Set<String> indexFiles = folderDetails.getFiles();
      String mergeFileName = folderDetails.getMergeFileName();
      if (null != mergeFileName) {
        String mergeIndexPath;
        if (entry.getValue().isRelative()) {
          mergeIndexPath =
              fileStore.getTablePath() + entry.getKey() + CarbonCommonConstants.FILE_SEPARATOR
                  + mergeFileName;
        } else {
          mergeIndexPath = entry.getKey() + CarbonCommonConstants.FILE_SEPARATOR + mergeFileName;
        }
        carbonIndexSize += FileFactory.getCarbonFile(mergeIndexPath).getSize();
      }
      for (String indexFile : indexFiles) {
        String indexPath;
        if (entry.getValue().isRelative()) {
          indexPath =
              fileStore.getTablePath() + entry.getKey() + CarbonCommonConstants.FILE_SEPARATOR
                  + indexFile;
        } else {
          indexPath = entry.getKey() + CarbonCommonConstants.FILE_SEPARATOR + indexFile;
        }
        carbonIndexSize += FileFactory.getCarbonFile(indexPath).getSize();
      }
    }
    return carbonIndexSize;
  }

  // Get the total size of carbon data and the total size of carbon index
  public static HashMap<String, Long> getDataSizeAndIndexSize(String tablePath,
      Segment segment) throws IOException {
    if (segment.getSegmentFileName() != null) {
      SegmentFileStore fileStore = new SegmentFileStore(tablePath, segment.getSegmentFileName());
      return getDataSizeAndIndexSize(fileStore, segment.isCarbonSegment());
    } else {
      return getDataSizeAndIndexSize(tablePath, segment.getSegmentNo());
    }
  }

  // Get the total size of segment.
  public static long getSizeOfSegment(String tablePath, Segment segment) throws IOException {
    HashMap<String, Long> dataSizeAndIndexSize = getDataSizeAndIndexSize(tablePath, segment);
    long size = 0;
    for (Long eachSize: dataSizeAndIndexSize.values()) {
      size += eachSize;
    }
    return size;
  }

  /**
   * Convert the bytes to base64 encode string
   * @param bytes
   * @return
   * @throws UnsupportedEncodingException
   */
  public static String encodeToString(byte[] bytes) throws UnsupportedEncodingException {
    return new String(Base64.encodeBase64(bytes),
        CarbonCommonConstants.DEFAULT_CHARSET);
  }

  /**
   * Decode string to byte array
   * @param objectString
   * @return
   * @throws UnsupportedEncodingException
   */
  public static byte[] decodeStringToBytes(String objectString)
      throws UnsupportedEncodingException {
    return Base64.decodeBase64(objectString.getBytes(CarbonCommonConstants.DEFAULT_CHARSET));
  }

  public static void copyCarbonDataFileToCarbonStorePath(
      String localFilePath,
      String targetPath, long fileSizeInBytes,
      DataLoadMetrics metrics) throws CarbonDataWriterException {
    if (targetPath.endsWith(".tmp") && localFilePath
        .endsWith(CarbonCommonConstants.FACT_FILE_EXT)) {
      // for partition case, write carbondata file directly to final path, keep index in temp path.
      // This can improve the commit job performance on s3a.
      targetPath =
          targetPath.substring(0, targetPath.lastIndexOf("/"));
      if (metrics != null) {
        metrics.addToPartitionPath(targetPath);
      }
    }
    long targetSize = copyCarbonDataFileToCarbonStorePath(localFilePath, targetPath,
        fileSizeInBytes);
    if (metrics != null) {
      // Storing the number of files written by each task.
      metrics.incrementCount();
      // Storing the files written by each task.
      metrics.addToOutputFiles(targetPath + CarbonCommonConstants.FILE_SEPARATOR + localFilePath
          .substring(localFilePath.lastIndexOf(File.separator) + 1) + ":" + targetSize);
      metrics.addOutputBytes(targetSize);
    }
  }

  /**
   * This method will copy the given file to carbon store location
   *
   * @param localFilePath local file name with full path
   * @throws CarbonDataWriterException
   * @return the file size
   */
  public static long copyCarbonDataFileToCarbonStorePath(String localFilePath,
      String carbonDataDirectoryPath, long fileSizeInBytes)
      throws CarbonDataWriterException {
    long copyStartTime = System.currentTimeMillis();
    LOGGER.info(String.format("Copying %s to %s, operation id %d", localFilePath,
        carbonDataDirectoryPath, copyStartTime));
    long targetSize = 0;
    try {
      CarbonFile localCarbonFile = FileFactory.getCarbonFile(localFilePath);
      long localFileSize = localCarbonFile.getSize();
      // the size of local carbon file must be greater than 0
      if (localFileSize == 0L) {
        LOGGER.error("The size of local carbon file: " + localFilePath + " is 0.");
        throw new CarbonDataWriterException("The size of local carbon file is 0.");
      }
      String carbonFilePath = carbonDataDirectoryPath + localFilePath
          .substring(localFilePath.lastIndexOf(File.separator));
      copyLocalFileToCarbonStore(carbonFilePath, localFilePath,
          CarbonCommonConstants.BYTEBUFFER_SIZE,
          getMaxOfBlockAndFileSize(fileSizeInBytes, localFileSize));
      CarbonFile targetCarbonFile = FileFactory.getCarbonFile(carbonFilePath);
      // the size of carbon file must be greater than 0
      // and the same as the size of local carbon file
      targetSize = targetCarbonFile.getSize();
      if (targetSize == 0L || targetSize != localFileSize) {
        LOGGER.error("The size of carbon file: " + carbonFilePath + " is 0 "
            + "or is not the same as the size of local carbon file: ("
            + "carbon file size=" + targetSize
            + ", local carbon file size=" + localFileSize + ")");
        throw new CarbonDataWriterException("The size of carbon file is 0 "
            + "or is not the same as the size of local carbon file.");
      }
    } catch (Exception e) {
      throw new CarbonDataWriterException(
          "Problem while copying file from local store to carbon store", e);
    }
    LOGGER.info(String.format("Total copy time is %d ms, operation id %d",
        System.currentTimeMillis() - copyStartTime, copyStartTime));
    return targetSize;
  }

  /**
   * This method will read the local carbon data file and write to carbon data file in HDFS
   *
   * @param carbonStoreFilePath
   * @param localFilePath
   * @param bufferSize
   * @param blockSize
   * @throws IOException
   */
  private static void copyLocalFileToCarbonStore(String carbonStoreFilePath, String localFilePath,
      int bufferSize, long blockSize) throws IOException {
    DataOutputStream dataOutputStream = null;
    DataInputStream dataInputStream = null;
    try {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("HDFS file block size for file: " + carbonStoreFilePath + " is " + blockSize
            + " (bytes");
      }
      dataOutputStream = FileFactory
          .getDataOutputStream(carbonStoreFilePath,
              bufferSize, blockSize);
      dataInputStream = FileFactory
          .getDataInputStream(localFilePath, bufferSize);
      IOUtils.copyBytes(dataInputStream, dataOutputStream, bufferSize);
    } finally {
      try {
        CarbonUtil.closeStream(dataInputStream);
        CarbonUtil.closeStream(dataOutputStream);
      } catch (IOException exception) {
        LOGGER.error(exception.getMessage(), exception);
      }
    }
  }

  /**
   * This method will return max of block size and file size
   *
   * @param blockSize
   * @param fileSize
   * @return
   */
  private static long getMaxOfBlockAndFileSize(long blockSize, long fileSize) {
    long maxSize = blockSize;
    if (fileSize > blockSize) {
      maxSize = fileSize;
    }
    // block size should be exactly divisible by 512 which is  maintained by HDFS as bytes
    // per checksum, dfs.bytes-per-checksum=512 must divide block size
    long remainder = maxSize % HDFS_CHECKSUM_LENGTH;
    if (remainder > 0) {
      maxSize = maxSize + HDFS_CHECKSUM_LENGTH - remainder;
    }
    // convert to make block size more readable.
    String readableBlockSize = ByteUtil.convertByteToReadable(blockSize);
    String readableFileSize = ByteUtil.convertByteToReadable(fileSize);
    String readableMaxSize = ByteUtil.convertByteToReadable(maxSize);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "The configured block size is " + readableBlockSize + ", the actual carbon file size is "
              + readableFileSize + ", choose the max value " + readableMaxSize
              + " as the block size on HDFS");
    }
    return maxSize;
  }

  /**
   * Generate the blockId as per the block path
   *
   * @param identifier
   * @param filePath
   * @param segmentId
   * @param isTransactionalTable
   * @param isStandardTable
   * @return
   */
  public static String getBlockId(AbsoluteTableIdentifier identifier, String filePath,
      String segmentId, boolean isTransactionalTable, boolean isStandardTable) {
    return getBlockId(identifier, filePath, segmentId, isTransactionalTable, isStandardTable,
        false);
  }

  /**
   * Generate the blockId as per the block path
   *
   * @return
   */
  public static String getBlockId(AbsoluteTableIdentifier identifier, String filePath,
      String segmentId, boolean isTransactionalTable, boolean isStandardTable,
      boolean isPartitionTable) {
    String blockId;
    String blockName = filePath.substring(filePath.lastIndexOf("/") + 1);
    String tablePath = identifier.getTablePath();

    if (filePath.startsWith(tablePath)) {
      if (!isTransactionalTable || isStandardTable) {
        blockId = "Part0" + CarbonCommonConstants.FILE_SEPARATOR + "Segment_" + segmentId
            + CarbonCommonConstants.FILE_SEPARATOR + blockName;
      } else {
        // This is the case with partition table.
        String partitionDir;
        int partLength = filePath.length() - blockName.length() - 1;
        if (tablePath.length() + 1 < partLength) {
          partitionDir =
              filePath.substring(tablePath.length() + 1, partLength);
        } else {
          partitionDir = "";
        }
        if (isPartitionTable) {
          blockId =
              partitionDir.replace(CarbonCommonConstants.FILE_SEPARATOR, "#")
                  + CarbonCommonConstants.FILE_SEPARATOR + blockName;
        } else {
          // Replace / with # on partition director to support multi level partitioning. And access
          // them all as a single entity.
          if (partitionDir.isEmpty()) {
            blockId = segmentId + CarbonCommonConstants.FILE_SEPARATOR + blockName;
          } else {
            blockId = partitionDir.replace(CarbonCommonConstants.FILE_SEPARATOR, "#")
                + CarbonCommonConstants.FILE_SEPARATOR + segmentId
                + CarbonCommonConstants.FILE_SEPARATOR + blockName;
          }

        }
      }
    } else {
      if (isPartitionTable) {
        // segment id can be null for external added partition path, so get id from blockname.
        segmentId = CarbonTablePath.DataFileUtil.getSegmentNo(blockName);
      }
      blockId = filePath.substring(0, filePath.length() - blockName.length()).replace("/", "#")
          + CarbonCommonConstants.FILE_SEPARATOR + "Segment_" + segmentId
          + CarbonCommonConstants.FILE_SEPARATOR + blockName;
    }
    return blockId;
  }

  /**
   * sets the local dictionary columns to wrapper schema, if the table property
   * local_dictionary_include is defined, then those columns will be set as local dictionary
   * columns, if not, all the no dictionary string datatype columns and varchar datatype columns are
   * set as local dictionary columns.
   * Handling for complexTypes::
   *    Since the column structure will be flat
   *    if the parent column is configured as local Dictionary column, then it gets the child column
   *    count and then sets the primitive child column as local dictionary column if it is string
   *    datatype column or varchar datatype column
   * Handling for both localDictionary Include and exclude columns:
   * There will basically be four scenarios which are
   * -------------------------------------------------------
   * | Local_Dictionary_include | Local_Dictionary_Exclude |
   * -------------------------------------------------------
   * |   Not Defined            |     Not Defined          |
   * |   Not Defined            |     Defined             |
   * |   Defined                |     Not Defined          |
   * |   Defined                |     Defined             |
   * -------------------------------------------------------
   * 1. when both local dictionary include and exclude are not defined, then set all the no
   * dictionary string datatype columns as local dictionary generate columns
   * 2. set all the no dictionary string and varchar datatype columns as local dictionary columns
   * except the columns present in local dictionary exclude
   * 3. & 4. when local dictionary include is defined, no need to check dictionary exclude columns
   * configured or not, we just need to set only the columns present in local dictionary include as
   * local dictionary columns
   *
   * @param columns
   * @param mainTableProperties
   */
  public static void setLocalDictColumnsToWrapperSchema(List<ColumnSchema> columns,
      Map<String, String> mainTableProperties, String isLocalDictEnabledForMainTable) {
    String[] listOfDictionaryIncludeColumns = null;
    String[] listOfDictionaryExcludeColumns = null;
    String localDictIncludeColumns =
        mainTableProperties.get(CarbonCommonConstants.LOCAL_DICTIONARY_INCLUDE);
    String localDictExcludeColumns =
        mainTableProperties.get(CarbonCommonConstants.LOCAL_DICTIONARY_EXCLUDE);
    if (null != localDictIncludeColumns) {
      listOfDictionaryIncludeColumns = localDictIncludeColumns.trim().split("\\s*,\\s*");
    }
    if (null != localDictExcludeColumns) {
      listOfDictionaryExcludeColumns = localDictExcludeColumns.trim().split("\\s*,\\s*");
    }
    if (null != isLocalDictEnabledForMainTable && Boolean
        .parseBoolean(isLocalDictEnabledForMainTable)) {
      int ordinal = 0;
      for (int i = 0; i < columns.size(); i++) {
        ColumnSchema column = columns.get(i);
        if (null == localDictIncludeColumns) {
          // if local dictionary exclude columns is not defined, then set all the no dictionary
          // string datatype column and varchar datatype columns
          if (null == localDictExcludeColumns) {
            // if column is complex type, call the setLocalDictForComplexColumns to set local
            // dictionary for all string and varchar child columns
            if (column.getDataType().isComplexType()) {
              ordinal = i + 1;
              ordinal = setLocalDictForComplexColumns(columns, ordinal, column.getNumberOfChild());
              i = ordinal - 1;
            } else {
              ordinal = i;
            }
            if (ordinal < columns.size()) {
              column = columns.get(ordinal);
            } else {
              continue;
            }
            // column should be no dictionary string datatype column or varchar datatype column
            if (column.isDimensionColumn() &&
                (column.getDataType().equals(DataTypes.STRING) ||
                    column.getDataType().equals(DataTypes.VARCHAR))) {
              column.setLocalDictColumn(true);
            }
            // if local dictionary exclude columns is defined, then set for all no dictionary string
            // datatype columns and varchar datatype columns except excluded columns
          } else {
            if (!Arrays.asList(listOfDictionaryExcludeColumns).contains(column.getColumnName())
                && column.getDataType().isComplexType()) {
              ordinal = i + 1;
              ordinal = setLocalDictForComplexColumns(columns, ordinal, column.getNumberOfChild());
              i = ordinal - 1;
            } else if (
                // if complex column is defined in Local Dictionary Exclude, then
                // unsetLocalDictForComplexColumns is mainly used to increment the ordinal value
                // required for traversing
                Arrays.asList(listOfDictionaryExcludeColumns).contains(column.getColumnName())
                    && column.getDataType().isComplexType()) {
              ordinal = i + 1;
              ordinal =
                  unsetLocalDictForComplexColumns(columns, ordinal, column.getNumberOfChild());
              i = ordinal - 1;
            } else {
              ordinal = i;

            }
            if (ordinal < columns.size()) {
              column = columns.get(ordinal);
            } else {
              continue;
            }
            //if column is primitive string or varchar and no dictionary column,then set local
            // dictionary if not specified as local dictionary exclude
            if (column.isDimensionColumn() &&
                (column.getDataType().equals(DataTypes.STRING) ||
                    column.getDataType().equals(DataTypes.VARCHAR))) {
              if (!Arrays.asList(listOfDictionaryExcludeColumns).contains(column.getColumnName())) {
                column.setLocalDictColumn(true);
              }
            }
          }
        } else {
          // if column is complex type, call the setLocalDictForComplexColumns to set local
          // dictionary for all string and varchar child columns which are defined in
          // local dictionary include
          if (localDictIncludeColumns.contains(column.getColumnName()) && column.getDataType()
              .isComplexType()) {
            ordinal = i + 1;
            ordinal = setLocalDictForComplexColumns(columns, ordinal, column.getNumberOfChild());
            i = ordinal - 1;
          } else {
            ordinal = i;
          }
          // if local dict columns are configured, set for all no dictionary string datatype or
          // varchar type column
          if (ordinal < columns.size()) {
            column = columns.get(ordinal);
          } else {
            continue;
          }
          if (column.isDimensionColumn() &&
              (column.getDataType().equals(DataTypes.STRING) ||
                  column.getDataType().equals(DataTypes.VARCHAR)) &&
              localDictIncludeColumns.toLowerCase()
                  .contains(column.getColumnName().toLowerCase())) {
            for (String dictColumn : listOfDictionaryIncludeColumns) {
              if (dictColumn.trim().equalsIgnoreCase(column.getColumnName())) {
                column.setLocalDictColumn(true);
              }
            }
          }
        }
      }
    }
  }

  /**
   * traverse through the columns of complex column specified in local dictionary include,
   * and set local dictionary for all the string and varchar child columns
   * @param allColumns
   * @param dimensionOrdinal
   * @param childColumnCount
   * @return
   */
  private static int setLocalDictForComplexColumns(List<ColumnSchema> allColumns,
      int dimensionOrdinal, int childColumnCount) {
    for (int i = 0; i < childColumnCount; i++) {
      ColumnSchema column = allColumns.get(dimensionOrdinal);
      if (column.getNumberOfChild() > 0) {
        dimensionOrdinal++;
        setLocalDictForComplexColumns(allColumns, dimensionOrdinal, column.getNumberOfChild());
      } else {
        if (column.isDimensionColumn() &&
            (column.getDataType().equals(DataTypes.STRING) ||
                column.getDataType().equals(DataTypes.VARCHAR))) {
          column.setLocalDictColumn(true);
        }
      }
      dimensionOrdinal++;
    }
    return dimensionOrdinal;
  }

  /**
   * traverse through the columns of complex column specified in local dictionary exclude
   * @param allColumns
   * @param dimensionOrdinal
   * @param childColumnCount
   * @return
   */
  private static int unsetLocalDictForComplexColumns(List<ColumnSchema> allColumns,
      int dimensionOrdinal, int childColumnCount) {
    for (int i = 0; i < childColumnCount; i++) {
      ColumnSchema column = allColumns.get(dimensionOrdinal);
      if (column.getNumberOfChild() > 0) {
        dimensionOrdinal++;
        // Dimension ordinal will take value from recursive functions so as to skip the
        // child columns of the complex column.
        dimensionOrdinal = unsetLocalDictForComplexColumns(allColumns, dimensionOrdinal,
            column.getNumberOfChild());
      } else {
        dimensionOrdinal++;
      }
    }
    return dimensionOrdinal;
  }

  /**
   * This method prepares a map which will have column and local dictionary generator mapping for
   * all the local dictionary columns.
   *
   * @param carbonTable
   * carbon Table
   */
  public static Map<String, LocalDictionaryGenerator> getLocalDictionaryModel(
      CarbonTable carbonTable) {
    List<ColumnSchema> wrapperColumnSchema = CarbonUtil
        .getColumnSchemaList(carbonTable.getVisibleDimensions(), carbonTable.getVisibleMeasures());
    boolean isLocalDictEnabled = carbonTable.isLocalDictionaryEnabled();
    // creates a map only if local dictionary is enabled, else map will be null
    Map<String, LocalDictionaryGenerator> columnLocalDictGenMap = new HashMap<>();
    if (isLocalDictEnabled) {
      int localDictionaryThreshold = carbonTable.getLocalDictionaryThreshold();
      for (ColumnSchema columnSchema : wrapperColumnSchema) {
        // check whether the column is local dictionary column or not
        if (columnSchema.isLocalDictColumn()) {
          columnLocalDictGenMap.put(columnSchema.getColumnName(),
              new ColumnLocalDictionaryGenerator(localDictionaryThreshold,
                  columnSchema.getDataType() == DataTypes.VARCHAR ?
                      CarbonCommonConstants.INT_SIZE_IN_BYTE :
                      CarbonCommonConstants.SHORT_SIZE_IN_BYTE));
        }
      }
    }
    if (isLocalDictEnabled) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Local dictionary is enabled for table: " + carbonTable.getTableUniqueName());
        LOGGER.debug(String.format("Local dictionary threshold for table %s is %d",
            carbonTable.getTableUniqueName(), carbonTable.getLocalDictionaryThreshold()));
      }
      Iterator<Map.Entry<String, LocalDictionaryGenerator>> iterator =
          columnLocalDictGenMap.entrySet().iterator();
      StringBuilder stringBuilder = new StringBuilder();
      while (iterator.hasNext()) {
        Map.Entry<String, LocalDictionaryGenerator> next = iterator.next();
        stringBuilder.append(next.getKey());
        stringBuilder.append(',');
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(String.format("Local dictionary will be generated for the columns: %s for"
                + " table %s", stringBuilder.toString(), carbonTable.getTableUniqueName()));
      }
    }
    return columnLocalDictGenMap;
  }

  /**
   * This method get the carbon file format version
   *
   * @param carbonTable
   * carbon Table
   */
  public static ColumnarFormatVersion getFormatVersion(CarbonTable carbonTable) throws IOException {
    String segmentPath = null;
    boolean supportFlatFolder = carbonTable.isSupportFlatFolder();
    CarbonIndexFileReader indexReader = new CarbonIndexFileReader();
    ColumnarFormatVersion version = null;
    SegmentIndexFileStore fileStore = new SegmentIndexFileStore();
    CarbonProperties carbonProperties = CarbonProperties.getInstance();
    // if the carbon table is support flat folder
    if (supportFlatFolder) {
      segmentPath = carbonTable.getTablePath();
      if (FileFactory.isFileExist(segmentPath)) {
        fileStore.readAllIIndexOfSegment(segmentPath);
        Map<String, byte[]> carbonIndexMap = fileStore.getCarbonIndexMap();
        if (carbonIndexMap.size() == 0) {
          version = carbonProperties.getFormatVersion();
        }
        for (byte[] fileData : carbonIndexMap.values()) {
          try {
            indexReader.openThriftReader(fileData);
            IndexHeader indexHeader = indexReader.readIndexHeader();
            version = ColumnarFormatVersion.valueOf((short)indexHeader.getVersion());
            break;
          } finally {
            indexReader.closeThriftReader();
          }
        }
      }
    } else {
      // get the valid segments
      SegmentStatusManager segmentStatusManager =
          new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier());
      SegmentStatusManager.ValidAndInvalidSegmentsInfo validAndInvalidSegmentsInfo =
          segmentStatusManager.getValidAndInvalidSegments(carbonTable.isMV());
      List<Segment> validSegments = validAndInvalidSegmentsInfo.getValidSegments();
      if (validSegments.isEmpty()) {
        return carbonProperties.getFormatVersion();
      }
      // get the carbon index file header from a valid segment
      for (Segment segment : validSegments) {
        segmentPath = carbonTable.getSegmentPath(segment.getSegmentNo());
        if (FileFactory.isFileExist(segmentPath)) {
          fileStore.readAllIIndexOfSegment(segmentPath);
          Map<String, byte[]> carbonIndexMap = fileStore.getCarbonIndexMap();
          if (carbonIndexMap.size() == 0) {
            LOGGER.warn("the valid segment path: " + segmentPath +
                " does not exist in the system of table: " + carbonTable.getTableUniqueName());
            continue;
          }
          for (byte[] fileData : carbonIndexMap.values()) {
            try {
              indexReader.openThriftReader(fileData);
              IndexHeader indexHeader = indexReader.readIndexHeader();
              version = ColumnarFormatVersion.valueOf((short)indexHeader.getVersion());
              break;
            } finally {
              indexReader.closeThriftReader();
            }
          }
          // if get the carbon file version from a valid segment, then end
          if (version != null) {
            break;
          }
        }
      }
      // if all valid segments path does not in the system,
      // then the carbon file version as default
      if (version == null) {
        version = CarbonProperties.getInstance().getFormatVersion();
      }
    }
    return version;
  }

  /**
   * Check if the page is adaptive encoded
   *
   * @param encodings
   * @return
   */
  public static boolean isEncodedWithMeta(List<org.apache.carbondata.format.Encoding> encodings) {
    if (encodings != null && !encodings.isEmpty()) {
      org.apache.carbondata.format.Encoding encoding = encodings.get(0);
      switch (encoding) {
        case DIRECT_COMPRESS:
        case DIRECT_STRING:
        case ADAPTIVE_INTEGRAL:
        case ADAPTIVE_DELTA_INTEGRAL:
        case ADAPTIVE_FLOATING:
        case ADAPTIVE_DELTA_FLOATING:
        case INT_LENGTH_COMPLEX_CHILD_BYTE_ARRAY:
          return true;
      }
    }
    return false;
  }

  /**
   * Check whether it is standard table means table path has Fact/Part0/Segment_ tail present with
   * all carbon files. In other cases carbon files present directly under table path or
   * tablePath/partition folder
   * TODO Read segment file and corresponding index file to get the correct carbondata file instead
   * of using this way.
   * @param table
   * @return
   */
  public static boolean isStandardCarbonTable(CarbonTable table) {
    return !(table.isSupportFlatFolder() || table.isHivePartitionTable());
  }

  /**
   * This method will form the FallbackEncodedColumnPage from input column page
   * @param columnPage actual data column page got from encoded column page if decoder based
   * fallback is disabled or newly created column page by extracting actual data from dictionary
   * data, if decoder based fallback is enabled
   * @param pageIndex pageIndex
   * @param columnSpec ColumnSpec
   * @return FallbackEncodedColumnPage
   * @throws IOException
   */
  public static FallbackEncodedColumnPage getFallBackEncodedColumnPage(ColumnPage columnPage,
      int pageIndex, TableSpec.ColumnSpec columnSpec) throws IOException {
    // new encoded column page
    EncodedColumnPage newEncodedColumnPage;

    switch (columnSpec.getColumnType()) {
      case COMPLEX_ARRAY:
      case COMPLEX_STRUCT:
      case COMPLEX:
        throw new RuntimeException("Unsupported DataType. Only COMPLEX_PRIMITIVE should come");

      case COMPLEX_PRIMITIVE:
        // for complex type column
        newEncodedColumnPage = ColumnPageEncoder.encodedColumn(columnPage);
        break;
      default:
        // for primitive column
        ColumnPageEncoder columnPageEncoder =
            DefaultEncodingFactory.getInstance().createEncoder(columnSpec, columnPage);
        newEncodedColumnPage = columnPageEncoder.encode(columnPage);
    }
    return new FallbackEncodedColumnPage(newEncodedColumnPage, pageIndex);
  }

  /**
   * Below method will be used to check whether particular encoding is present
   * in the dimension or not
   *
   * @param encoding encoding to search
   * @return if encoding is present in dimension
   */
  public static boolean hasEncoding(List<org.apache.carbondata.format.Encoding> encodings,
      org.apache.carbondata.format.Encoding encoding) {
    return encodings.contains(encoding);
  }

  /**
   * Below method will be used to create the inverted index reverse
   * this will be used to point to actual data in the chunk
   *
   * @param invertedIndex inverted index
   * @return reverse inverted index
   */
  public static int[] getInvertedReverseIndex(int[] invertedIndex) {
    int[] columnIndexTemp = new int[invertedIndex.length];

    for (int i = 0; i < invertedIndex.length; i++) {
      columnIndexTemp[invertedIndex[i]] = i;
    }
    return columnIndexTemp;
  }

  /**
   * Below method is to generateUUID (Random Based)
   * later it will be extended for TimeBased,NameBased
   *
   * @return UUID as String
   */
  public static String generateUUID() {
    return UUID.randomUUID().toString();
  }

  public static String getIndexServerTempPath() {
    String tempFolderPath = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_INDEX_SERVER_TEMP_PATH);
    if (null == tempFolderPath) {
      tempFolderPath =
          "/tmp/" + CarbonCommonConstants.INDEX_SERVER_TEMP_FOLDER_NAME;
    } else {
      tempFolderPath =
          tempFolderPath + CarbonCommonConstants.FILE_SEPARATOR
              + CarbonCommonConstants.INDEX_SERVER_TEMP_FOLDER_NAME;
    }
    return CarbonUtil.checkAndAppendFileSystemURIScheme(tempFolderPath);
  }

  public static CarbonFile createTempFolderForIndexServer(String queryId)
          throws IOException {
    final String path = getIndexServerTempPath();
    if (!FileFactory.isFileExist(path)) {
      // Create the new index server temp directory if it does not exist
      LOGGER.info("Creating Index Server temp folder:" + path);
      FileFactory.createDirectoryAndSetPermission(path,
              new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    }
    if (queryId == null) {
      return null;
    }
    CarbonFile file = FileFactory.getCarbonFile(path + CarbonCommonConstants.FILE_SEPARATOR
            + queryId);
    if (!file.mkdirs()) {
      LOGGER.info("Unable to create table directory: " + path + CarbonCommonConstants.FILE_SEPARATOR
              + queryId);
      return null;
    } else {
      LOGGER.info("Successfully Created directory: " + path + CarbonCommonConstants.FILE_SEPARATOR
              + queryId);
      return file;
    }
  }

  /**
   * For alter table tableProperties of long string, modify ColumnSchema
   * set operation:
   * a. reset the current varchar datatype to string
   * b. reorder back to original order (consider sort column and original schema ordinal)
   * c. set the new columns to varchar
   * d. reorder new varchar columns to be after dimensions
   * <p>
   * unset operation:
   * a. reset the current varchar datatype to string
   * b. reorder back to original order (consider sort column and original schema ordinal)
   *
   * @param columns                 list of column schema from thrift
   * @param longStringColumnsString comma separated long string columns to be set, empty for unset
   * @return updated columnSchema list
   */
  public static List<org.apache.carbondata.format.ColumnSchema> reorderColumnsForLongString(
      List<org.apache.carbondata.format.ColumnSchema> columns, String longStringColumnsString) {
    // schema will be like
    // sortColumns-otherDimensions-varchar-[complexColumns + complex_child - measures]
    List<org.apache.carbondata.format.ColumnSchema> sortColumns = new ArrayList<>();
    List<org.apache.carbondata.format.ColumnSchema> dimColumns = new ArrayList<>();
    List<org.apache.carbondata.format.ColumnSchema> otherColumns = new ArrayList<>();
    // true if cleared the varchar type in dims to string
    boolean isResetDone = false;
    // complex type child also looks like dimension or measure,
    // so just take all other columns at once.
    int otherColumnStartIndex = -1;
    for (int i = 0; i < columns.size(); i++) {
      Map<String, String> columnProperties = columns.get(i).getColumnProperties();
      if (columnProperties != null
          && columnProperties.get(CarbonCommonConstants.SORT_COLUMNS) != null
          && columnProperties.get(CarbonCommonConstants.SORT_COLUMNS).equalsIgnoreCase("true")) {
        // add sort column dimensions
        sortColumns.add(columns.get(i));
      } else if ((columns.get(i).getData_type() == org.apache.carbondata.format.DataType.ARRAY
          || columns.get(i).getData_type() == org.apache.carbondata.format.DataType.STRUCT
          || columns.get(i).getData_type() == org.apache.carbondata.format.DataType.MAP || (!columns
          .get(i).isDimension()))) {
        // complex type or measure starts here and processed all sort columns and other dimensions
        otherColumnStartIndex = i;
        break;
      } else {
        // if it is varchar, reset to string as new set and unset needs to be done.
        // if not, just add it to dimColumns
        org.apache.carbondata.format.ColumnSchema col = columns.get(i);
        if (col.data_type == org.apache.carbondata.format.DataType.VARCHAR) {
          col.data_type = org.apache.carbondata.format.DataType.STRING;
          isResetDone = true;
        }
        dimColumns.add(col);
      }
    }
    if (otherColumnStartIndex != -1) {
      otherColumns.addAll(columns.subList(otherColumnStartIndex, columns.size()));
    }
    if (isResetDone) {
      // reorder the dims based on original schema ordinal
      dimColumns.sort(new Comparator<org.apache.carbondata.format.ColumnSchema>() {
        @Override
        public int compare(org.apache.carbondata.format.ColumnSchema o1,
            org.apache.carbondata.format.ColumnSchema o2) {
          return o1.getSchemaOrdinal() - o2.getSchemaOrdinal();
        }
      });
    }
    List<org.apache.carbondata.format.ColumnSchema> nonVarCharDims = new ArrayList<>();
    // for setting long string columns
    if (!longStringColumnsString.isEmpty()) {
      String[] inputColumns = longStringColumnsString.split(",");
      inputColumns = Arrays.stream(inputColumns).map(String::trim)
              .map(String::toLowerCase).toArray(String[]::new);
      Set<String> longStringSet = new HashSet<>(Arrays.asList(inputColumns));
      List<org.apache.carbondata.format.ColumnSchema> varCharColumns = new ArrayList<>();
      // change data type to varchar and extract the varchar columns
      for (org.apache.carbondata.format.ColumnSchema dim : dimColumns) {
        if (longStringSet.contains(dim.getColumn_name())) {
          dim.data_type = org.apache.carbondata.format.DataType.VARCHAR;
          // extract varchar dimensions
          varCharColumns.add(dim);
        } else {
          // extract non varchar, non sort dimensions
          nonVarCharDims.add(dim);
        }
      }
      // append varchar in the end of dimensions
      nonVarCharDims.addAll(varCharColumns);
    } else {
      nonVarCharDims = dimColumns;
    }
    // combine all columns and return
    if (otherColumns.size() != 0) {
      nonVarCharDims.addAll(otherColumns);
    }
    sortColumns.addAll(nonVarCharDims);
    return sortColumns;
  }

  /**
   * get cache expiration time from carbonTable table properties
   */
  public static long getExpiration_time(CarbonTable carbonTable) {
    String cacheExpirationTime = carbonTable.getTableInfo().getFactTable().getTableProperties()
        .get(CarbonCommonConstants.INDEX_CACHE_EXPIRATION_TIME_IN_SECONDS);
    if (null == cacheExpirationTime) {
      return CarbonCommonConstants.INDEX_CACHE_EXPIRATION_TIME_IN_SECONDS_DEFAULT;
    }
    return Integer.parseInt(cacheExpirationTime);
  }

  /**
   * delete the folder depend on the queryId
   * @param queryId query id
   * @throws IOException
   */
  public static void deleteTempFolderForIndexServer(String queryId)
          throws IOException {
    final String path = getIndexServerTempPath();
    if (queryId == null) {
      return;
    }
    String pathName = path + CarbonCommonConstants.FILE_SEPARATOR + queryId;
    String indexServerTmpDirPath = CarbonUtil
            .checkAndAppendFileSystemURIScheme(pathName);
    if (!FileFactory
            .deleteFile(indexServerTmpDirPath)) {
      LOGGER.info("Unable to delete directory: " + pathName);
    } else {
      LOGGER.info("Successfully delete directory: " + pathName);
    }
  }

  /**
   * use to clean the tmp folder, avoid exceeding the limit on the number of files.
   */
  public static void cleanTempFolderForIndexServer()throws IOException  {
    final String folderPath = getIndexServerTempPath();
    String indexServerTmpDirPath = CarbonUtil
            .checkAndAppendFileSystemURIScheme(folderPath);
    if (FileFactory.deleteFile(indexServerTmpDirPath)) {
      LOGGER.info("Complete " + indexServerTmpDirPath + " file cleanup.");
    } else {
      LOGGER.info("Failed " + indexServerTmpDirPath + " file cleanup.");
    }
  }

  /**
   * use to clean the tmp folder, avoid exceeding the limit on the number of files.
   */
  public static void agingTempFolderForIndexServer(long agingTime)throws
          IOException, InterruptedException {
    final String folderPath = getIndexServerTempPath();
    if (FileFactory.isFileExist(folderPath)) {
      List<CarbonFile> carbonFileList = FileFactory.getFolderList(folderPath);
      carbonFileList.stream().filter(carbonFile -> carbonFile.getLastModifiedTime() < agingTime
      ).forEach(delFile -> {
        try {
          deleteFoldersAndFiles(delFile);
          LOGGER.info("delete file + " + delFile.getPath());
        } catch (IOException | InterruptedException e) {
          LOGGER.error("aging temp folder for index server failed.");
        }
      });
    }
  }

  public static void updateNullValueBasedOnDatatype(DataOutputStream dataOutputStream,
      DataType dataType) throws IOException {
    if (dataType == DataTypes.STRING || dataType == DataTypes.VARCHAR) {
      if (DataTypeUtil.isByteArrayComplexChildColumn(dataType)) {
        dataOutputStream.writeInt(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length);
      } else {
        dataOutputStream.writeShort(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length);
      }
      dataOutputStream.write(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY);
    } else {
      if (DataTypeUtil.isByteArrayComplexChildColumn(dataType)) {
        dataOutputStream.writeInt(CarbonCommonConstants.EMPTY_BYTE_ARRAY.length);
      } else {
        dataOutputStream.writeShort(CarbonCommonConstants.EMPTY_BYTE_ARRAY.length);
      }
      dataOutputStream.write(CarbonCommonConstants.EMPTY_BYTE_ARRAY);
    }
  }

  /**
   * returns whether column is complex column based on column name for child column
   * @return true if column is complex
   */
  public static boolean isComplexColumn(String colName) {
    return colName.contains(".val") || colName.contains(CarbonCommonConstants.POINT);
  }

  /**
   * This method returns the minmax value from the index row object
   * @return minmax byte array
   */
  public static byte[][] getMinMaxValue(IndexRow row, int index) {
    IndexRow minMaxRow = row.getRow(index);
    byte[][] minMax = new byte[minMaxRow.getColumnCount()][];
    for (int i = 0; i < minMax.length; i++) {
      minMax[i] = minMaxRow.getByteArray(i);
    }
    return minMax;
  }
}
