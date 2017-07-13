/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.urey.flume.source.taildir;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import com.google.gson.stream.JsonReader;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.client.avro.ReliableEventReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReliableTaildirEventReader implements ReliableEventReader {
  private static final Logger logger = LoggerFactory.getLogger(ReliableTaildirEventReader.class);

  private final Table<String, File, Pattern> tailFileTable;
  private final Table<String, String, String> headerTable;

  private TailFile currentFile = null;
  private Map<Long, TailFile> tailFiles = Maps.newHashMap();
  private long updateTime;
  private boolean addByteOffset;
  private boolean committed = true;

  private final boolean recursiveDirectorySearch;
  private final boolean annotateYarnApplicationId;
  private final boolean annotateYarnContainerId;

  private final boolean annotateFileName;
  private final String fileNameHeader;

  /**
   * Create a ReliableTaildirEventReader to watch the given directory.
   */
  private ReliableTaildirEventReader(Map<String, String> filePaths,
      Table<String, String, String> headerTable, String positionFilePath,
      boolean skipToEnd, boolean addByteOffset,
      boolean recursiveDirectorySearch,
      boolean annotateYarnApplicationId,
      boolean annotateYarnContainerId,
      boolean annotateFileName, String fileNameHeader) throws IOException {
    // Sanity checks
    Preconditions.checkNotNull(filePaths);
    Preconditions.checkNotNull(positionFilePath);

    if (logger.isDebugEnabled()) {
      logger.debug("Initializing {} with directory={}, metaDir={}",
          new Object[] { ReliableTaildirEventReader.class.getSimpleName(), filePaths });
    }

    Table<String, File, Pattern> tailFileTable = HashBasedTable.create();
    for (Entry<String, String> e : filePaths.entrySet()) {
      File f = new File(e.getValue());
      File parentDir =  f.getParentFile();
      Preconditions.checkState(parentDir.exists(),
        "Directory does not exist: " + parentDir.getAbsolutePath());
      Pattern fileNamePattern = Pattern.compile(f.getName());
      tailFileTable.put(e.getKey(), parentDir, fileNamePattern);
    }
    logger.info("tailFileTable: " + tailFileTable.toString());
    logger.info("headerTable: " + headerTable.toString());

    this.tailFileTable = tailFileTable;
    this.headerTable = headerTable;
    this.addByteOffset = addByteOffset;

    this.recursiveDirectorySearch = recursiveDirectorySearch;
    this.annotateYarnApplicationId = annotateYarnApplicationId;
    this.annotateYarnContainerId = annotateYarnContainerId;

    this.annotateFileName = annotateFileName;
    this.fileNameHeader = fileNameHeader;

    updateTailFiles(skipToEnd);

    logger.info("Updating position from position file: " + positionFilePath);
    loadPositionFile(positionFilePath);
  }

  /**
   * Load a position file which has the last read position of each file.
   * If the position file exists, update tailFiles mapping.
   */
  public void loadPositionFile(String filePath) {
    Long inode, pos;
    String path;
    FileReader fr = null;
    JsonReader jr = null;
    try {
      fr = new FileReader(filePath);
      jr = new JsonReader(fr);
      jr.beginArray();
      while (jr.hasNext()) {
        inode = null;
        pos = null;
        path = null;
        jr.beginObject();
        while (jr.hasNext()) {
          switch (jr.nextName()) {
          case "inode":
            inode = jr.nextLong();
            break;
          case "pos":
            pos = jr.nextLong();
            break;
          case "file":
            path = jr.nextString();
            break;
          }
        }
        jr.endObject();

        for (Object v : Arrays.asList(inode, pos, path)) {
          Preconditions.checkNotNull(v, "Detected missing value in position file. "
              + "inode: " + inode + ", pos: " + pos + ", path: " + path);
        }
        TailFile tf = tailFiles.get(inode);
        if (tf != null && tf.updatePos(tf.getPath(), inode, pos)) {
          tailFiles.put(inode, tf);
        } else {
          logger.info("Missing file: " + path + ", inode: " + inode + ", pos: " + pos);
        }
      }
      jr.endArray();
    } catch (FileNotFoundException e) {
      logger.info("File not found: " + filePath + ", not updating position");
    } catch (IOException e) {
      logger.error("Failed loading positionFile: " + filePath, e);
    } finally {
      try {
        if (fr != null) fr.close();
        if (jr != null) jr.close();
      } catch (IOException e) {
        logger.error("Error: " + e.getMessage(), e);
      }
    }
  }

  public Map<Long, TailFile> getTailFiles() {
    return tailFiles;
  }

  public void setCurrentFile(TailFile currentFile) {
    this.currentFile = currentFile;
  }

  @Override
  public Event readEvent() throws IOException {
    List<Event> events = readEvents(1);
    if (events.isEmpty()) {
      return null;
    }
    return events.get(0);
  }

  @Override
  public List<Event> readEvents(int numEvents) throws IOException {
    return readEvents(numEvents, false);
  }

  @VisibleForTesting
  public List<Event> readEvents(TailFile tf, int numEvents) throws IOException {
    setCurrentFile(tf);
    return readEvents(numEvents, true);
  }

  public List<Event> readEvents(int numEvents, boolean backoffWithoutNL)
      throws IOException {
    if (!committed) {
      if (currentFile == null) {
        throw new IllegalStateException("current file does not exist. " + currentFile.getPath());
      }
      logger.info("Last read was never committed - resetting position");
      long lastPos = currentFile.getPos();
      currentFile.updateFilePos(lastPos);
    }
    List<Event> events = currentFile.readEvents(numEvents, backoffWithoutNL, addByteOffset);
    if (events.isEmpty()) {
      return events;
    }

    if (annotateFileName) {
      String filename = currentFile.getPath();
      for (Event event : events) {
        event.getHeaders().put(fileNameHeader, filename);
      }
    }

    if(annotateYarnApplicationId) {
      String filename = currentFile.getPath();
      String yarnApplicationId = getApplicationId(filename);
      if(yarnApplicationId == null) yarnApplicationId = "";
      for(Event event: events) {
        event.getHeaders().put("yarn_application_id", yarnApplicationId);
      }
    }

    if(annotateYarnContainerId) {
      String filename = currentFile.getPath();
      String yarnContainerId = getContainerId(filename);
      if(yarnContainerId == null) yarnContainerId = "";
      for(Event event: events) {
        event.getHeaders().put("yarn_container_id", yarnContainerId);
      }
    }

    Map<String, String> headers = currentFile.getHeaders();
    if (headers != null && !headers.isEmpty()) {
      for (Event event : events) {
        event.getHeaders().putAll(headers);
      }
    }
    committed = false;
    return events;
  }

  /**
   *
   * @param path
   * @return
   */
  private String getApplicationId(String path) {
    String[] dirs = path.split("/");
    for(String dir: dirs) {
      if(dir.startsWith("application_")) {
        return dir;
      }
    }
    return null;
  }

  /**
   *
   * @param path
   * @return
   */
  private String getContainerId(String path) {
    String[] dirs = path.split("/");
    for(String dir: dirs) {
      if(dir.startsWith("container_")) {
        return dir;
      }
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    for (TailFile tf : tailFiles.values()) {
      if (tf.getRaf() != null) tf.getRaf().close();
    }
  }

  /** Commit the last lines which were read. */
  @Override
  public void commit() throws IOException {
    if (!committed && currentFile != null) {
      long pos = currentFile.getLineReadPos();
      currentFile.setPos(pos);
      currentFile.setLastUpdated(updateTime);
      committed = true;
    }
  }

  /**
   * Update tailFiles mapping if a new file is created or appends are detected
   * to the existing file.
   */
  public List<Long> updateTailFiles(boolean skipToEnd) throws IOException {
    updateTime = System.currentTimeMillis();
    List<Long> updatedInodes = Lists.newArrayList();

    for (Cell<String, File, Pattern> cell : tailFileTable.cellSet()) {
      Map<String, String> headers = headerTable.row(cell.getRowKey());
      File parentDir = cell.getColumnKey();
      Pattern fileNamePattern = cell.getValue();

      for (File f : getMatchFiles(parentDir, fileNamePattern)) {
        long inode = getInode(f);
        TailFile tf = tailFiles.get(inode);
        if (tf == null) {
          long startPos = skipToEnd ? f.length() : 0;
          tf = openFile(f, headers, inode, startPos);
        } else{
          boolean updated = tf.getLastUpdated() < f.lastModified();
          if (updated) {
            if (tf.getRaf() == null) {
              tf = openFile(f, headers, inode, tf.getPos());
            }
            if (f.length() < tf.getPos()) {
              logger.info("Pos " + tf.getPos() + " is larger than file size! "
                  + "Restarting from pos 0, file: " + tf.getPath() + ", inode: " + inode);
              tf.updatePos(tf.getPath(), inode, 0);
            }
          }
          //modify by yangzhe begin
          if (!tf.getPath().equals(f.getAbsolutePath())) {
            tf = openFile(f, headers, inode, tf.getPos());
          }
          //modify by yangzhe end
          tf.setNeedTail(updated);
        }
        tailFiles.put(inode, tf);
        updatedInodes.add(inode);
      }
    }
    return updatedInodes;
  }

  public List<Long> updateTailFiles() throws IOException {
    return updateTailFiles(false);
  }

  /**
   Filter to exclude files/directories either hidden, finished, or names matching the ignore pattern
   */
//  final FileFilter filter = new FileFilter() {
//    public boolean accept(File candidate) {
//      if ( candidate.isDirectory() ) {
//        String directoryName = candidate.getName();
//        if ( (! recursiveDirectorySearch) ||
//                (directoryName.startsWith(".")) ||
//                ignorePattern.matcher(directoryName).matches()) {
//          return false;
//        }
//        return true;
//      }
//      else{
//        String fileName = candidate.getName();
//        if ((fileName.endsWith(completedSuffix)) ||
//                (fileName.startsWith(".")) ||
//                ignorePattern.matcher(fileName).matches()) {
//          return false;
//        }
//      }
//      return true;
//    }
//  };

  /**
   * Recursively gather candidate files
   * @param directory the directory to gather files from
   * @return list of files within the passed in directory
   */
  private  List<File> getCandidateFiles(File directory, FileFilter filter){
    List<File> candidateFiles = new ArrayList<File>();
    if(directory == null || !directory.isDirectory()){
      return candidateFiles;
    }
    for(File file : directory.listFiles(filter)){
      if (file.isDirectory()) {
        candidateFiles.addAll(getCandidateFiles(file, filter));
      }
      else {
        candidateFiles.add(file);
      }
    }
    return candidateFiles;
  }

  private List<File> getMatchFiles(File parentDir, final Pattern fileNamePattern) {

//    FileFilter filter = new FileFilter() {
//      public boolean accept(File f) {
//        String fileName = f.getName();
//        if (f.isDirectory() || !fileNamePattern.matcher(fileName).matches()) {
//          return false;
//        }
//        return true;
//      }
//    };
//    File[] files = parentDir.listFiles(filter);
//    ArrayList<File> result = Lists.newArrayList(files);

    FileFilter filter = new FileFilter() {
      public boolean accept(File candidate) {
        if ( candidate.isDirectory() ) {
          String directoryName = candidate.getName();
          if ( (! recursiveDirectorySearch) ||
                  (directoryName.startsWith("."))) {
            return false;
          }
          return true;
        }
        else{
          String fileName = candidate.getName();
          if ( !fileNamePattern.matcher(fileName).matches() ) {
            return false;
          }
        }
        return true;
      }
    };
    List<File> candidateFiles = Collections.emptyList();
    candidateFiles = getCandidateFiles(parentDir, filter);

    ArrayList<File> result = Lists.newArrayList(candidateFiles);
    Collections.sort(result, new TailFile.CompareByLastModifiedTime());
    return result;
  }

  private long getInode(File file) throws IOException {
    long inode = (long) Files.getAttribute(file.toPath(), "unix:ino");
//    long inode = Long.valueOf(String.valueOf(Files.getAttribute(file.toPath(), "unix:ino"))).longValue();
    return inode;
  }

  private TailFile openFile(File file, Map<String, String> headers, long inode, long pos) {
    try {
      logger.info("Opening file: " + file + ", inode: " + inode + ", pos: " + pos);
      return new TailFile(file, headers, inode, pos);
    } catch (IOException e) {
      throw new FlumeException("Failed opening file: " + file, e);
    }
  }

  /**
   * Special builder class for ReliableTaildirEventReader
   */
  public static class Builder {
    private Map<String, String> filePaths;
    private Table<String, String, String> headerTable;
    private String positionFilePath;
    private boolean skipToEnd;
    private boolean addByteOffset;

    private boolean recursiveDirectorySearch = TaildirSourceConfigurationConstants.DEFAULT_RECURSIVE_DIRECTORY_SEARCH;
    private Boolean annotateYarnApplicationId = TaildirSourceConfigurationConstants.DEFAULT_YARN_APPLICATION_HEADER;
    private Boolean annotateYarnContainerId = TaildirSourceConfigurationConstants.DEFAULT_YARN_CONTAINER_HEADER;

    private Boolean annotateFileName = TaildirSourceConfigurationConstants.DEFAULT_FILE_HEADER;
    private String fileNameHeader = TaildirSourceConfigurationConstants.DEFAULT_FILENAME_HEADER_KEY;

    public Builder filePaths(Map<String, String> filePaths) {
      this.filePaths = filePaths;
      return this;
    }

    public Builder headerTable(Table<String, String, String> headerTable) {
      this.headerTable = headerTable;
      return this;
    }

    public Builder positionFilePath(String positionFilePath) {
      this.positionFilePath = positionFilePath;
      return this;
    }

    public Builder skipToEnd(boolean skipToEnd) {
      this.skipToEnd = skipToEnd;
      return this;
    }

    public Builder addByteOffset(boolean addByteOffset) {
      this.addByteOffset = addByteOffset;
      return this;
    }

    public Builder annotateYarnApplicationId(Boolean annotateYarnApplicationId) {
      this.annotateYarnApplicationId = annotateYarnApplicationId;
      return this;
    }

    public Builder annotateYarnContainerId(Boolean annotateYarnContainerId) {
      this.annotateYarnContainerId = annotateYarnContainerId;
      return this;
    }

    public Builder recursiveDirectorySearch(boolean recursiveDirectorySearch) {
      this.recursiveDirectorySearch = recursiveDirectorySearch;
      return this;
    }

    public Builder annotateFileName(Boolean annotateFileName) {
      this.annotateFileName = annotateFileName;
      return this;
    }

    public Builder fileNameHeader(String fileNameHeader) {
      this.fileNameHeader = fileNameHeader;
      return this;
    }

    public ReliableTaildirEventReader build() throws IOException {
      return new ReliableTaildirEventReader(filePaths, headerTable, positionFilePath, skipToEnd, addByteOffset,
              recursiveDirectorySearch, annotateYarnApplicationId,
              annotateYarnContainerId, annotateFileName, fileNameHeader);
    }
  }

}
