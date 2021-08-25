// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.connection;

import com.linkedin.cdi.configuration.MultistageProperties;
import com.linkedin.cdi.exception.RetriableAuthenticationException;
import com.linkedin.cdi.factory.ConnectionClientFactory;
import com.linkedin.cdi.factory.sftp.SftpClient;
import com.linkedin.cdi.keys.ExtractorKeys;
import com.linkedin.cdi.keys.JobKeys;
import com.linkedin.cdi.keys.SftpKeys;
import com.linkedin.cdi.util.InputStreamUtils;
import com.linkedin.cdi.util.WorkUnitStatus;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.configuration.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Slf4j
public class SftpConnection extends MultistageConnection {
  private static final Logger LOG = LoggerFactory.getLogger(SftpConnection.class);

  final private SftpKeys sftpSourceKeys;
  SftpClient fsClient;

  public SftpConnection(State state, JobKeys jobKeys, ExtractorKeys extractorKeys) {
    super(state, jobKeys, extractorKeys);
    assert jobKeys instanceof SftpKeys;
    sftpSourceKeys = (SftpKeys) jobKeys;
  }

  @Override
  public WorkUnitStatus execute(WorkUnitStatus status) {
    return null;
  }

  @Override
  public boolean closeAll(String message) {
    if (this.fsClient != null) {
      log.info("Shutting down FileSystem connection");
      this.fsClient.close();
      fsClient = null;
    }
    return true;
  }

  /**
   This method is the main method to list files based on source base directory and source entity
   ms.source.file.pattern
   if Is not blank:
   List the files and output as CSV
   if is blank:
   ms.extract.target.file.name?
   if is blank:
   List the files and output as CSV
   if is not blank
   if file size is 1
   Dump the file
   if files size is >1
   Dump only the file which matches the pattern
   */
  @Override
  public WorkUnitStatus executeFirst(WorkUnitStatus workUnitStatus) throws RetriableAuthenticationException {
    WorkUnitStatus status = super.executeFirst(workUnitStatus);
    String path = getPath();
    String finalPrefix = getWorkUnitSpecificString(path, getExtractorKeys().getDynamicParameters());
    log.info("File path found is: " + finalPrefix);
    try {
      if (getFsClient() == null) {
        log.error("Error initializing SFTP connection");
        return null;
      }
    } catch (Exception e) {
      log.error("Error initializing SFTP connection", e);
      return null;
    }

    //get List of files matching the pattern
    List<String> files;
    try {
       files = getFiles(finalPrefix);
    } catch (Exception e) {
      log.error("Error reading file list", e);
      return null;
    }

    boolean isFileWithPrefixExist = files.stream().anyMatch(file -> file.equals(finalPrefix));
    log.info("No Of Files to be processed matching the pattern: {}", files.size());
    if (StringUtils.isNotBlank(sftpSourceKeys.getFilesPattern())) {
      status.setBuffer(InputStreamUtils.convertListToInputStream(getFilteredFiles(files)));
    } else {
      if (StringUtils.isBlank(sftpSourceKeys.getTargetFilePattern())) {
        status.setBuffer(InputStreamUtils.convertListToInputStream(files));
      } else {
        String fileToDownload = "";
        if (files.size() == 1) {
          fileToDownload = files.get(0);
        } else if (isFileWithPrefixExist) {
          fileToDownload = finalPrefix;
        }
        if (StringUtils.isNotBlank(fileToDownload)) {
          log.info("Downloading file: {}", files.get(0));
          try {
            status.setBuffer(this.fsClient.getFileStream(fileToDownload));
          } catch (Exception e) {
            log.error("Error downloading file {}", fileToDownload, e);
            return null;
          }
        } else {
          log.warn("Invalid set of parameters. Please make sure to set source directory, entity and file pattern");
        }
      }
    }
    return status;
  }

  private SftpClient getFsClient() {
    if (this.fsClient == null) {
      try {
        Class<?> factoryClass = Class.forName(MultistageProperties.MSTAGE_CONNECTION_CLIENT_FACTORY.getValidNonblankWithDefault(this.getState()));
        ConnectionClientFactory factory = (ConnectionClientFactory) factoryClass.getDeclaredConstructor().newInstance();
        this.fsClient = factory.getSftpChannelClient(this.getState());
      } catch (Exception e) {
        LOG.error("Error initiating SFTP client", e);
      }
    }
    return this.fsClient;
  }

  /**
   * //TODO: List files based on pattern on parent nodes as well.
   * The current version supports pattern only on leaf node.
   * Ex: file path supported "/a/b/*c*"
   * file path not supported "/a/*b/*c*
   * Get files list based on pattern
   * @param filesPattern pattern of content to list
   * @return list of content
   */
  private List<String> getFiles(String filesPattern) {
    List<String> files = new ArrayList<>();
    log.info("Files to be processed from input " + filesPattern);
    try {
      files = fsClient.ls(filesPattern);
      int i = 0;
      for (String file : files) {
        URI uri = new URI(file);
        String filepath = uri.toString();
        if (!uri.isAbsolute()) {
          File f = new File(getBaseDir(filesPattern), filepath);
          filepath = f.getAbsolutePath();
        }
        files.set(i, filepath);
        i++;
      }
    } catch (Exception e) {
      log.error("Unable to list files " + e.getMessage());
    }
    return files;
  }
  private String getPath() {
    return sftpSourceKeys.getFilesPath();
  }

  private List<String> getFilteredFiles(List<String> files) {
    return files.stream().filter(file -> file.matches(sftpSourceKeys.getFilesPattern())).collect(Collectors.toList());
  }

  private String getBaseDir(String uri) {
    File file = new File(uri);
    return file.getParentFile().getAbsolutePath() + sftpSourceKeys.getPathSeparator();
  }

}
