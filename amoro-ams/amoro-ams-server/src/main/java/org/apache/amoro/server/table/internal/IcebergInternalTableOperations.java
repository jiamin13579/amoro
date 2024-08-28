/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.amoro.server.table.internal;

import org.apache.amoro.api.ServerTableIdentifier;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.mapper.TableMetaMapper;
import org.apache.amoro.server.utils.InternalTableUtil;
import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.*;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Iceberg table operations {@link TableOperations} */
public class IcebergInternalTableOperations extends PersistentBase implements TableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergInternalTableOperations.class);

  private final ServerTableIdentifier identifier;

  private TableMetadata current;
  private final FileIO io;
  private org.apache.amoro.server.table.TableMetadata tableMetadata;

  public IcebergInternalTableOperations(
      ServerTableIdentifier identifier,
      org.apache.amoro.server.table.TableMetadata tableMetadata,
      FileIO io) {
    this.io = io;
    this.tableMetadata = tableMetadata;
    this.identifier = identifier;
  }

  @Override
  public TableMetadata current() {
    if (this.current == null) {
      this.refresh();
    }
    return this.current;
  }

  @Override
  public TableMetadata refresh() {
    if (this.tableMetadata == null) {
      this.tableMetadata =
          getAs(
              TableMetaMapper.class, mapper -> mapper.selectTableMetaById(this.identifier.getId()));
    }
    if (this.tableMetadata == null) {
      return null;
    }
    String metadataFileLocation = tableMetadataLocation(this.tableMetadata);
    if (StringUtils.isBlank(metadataFileLocation)) {
      return null;
    }
    this.current = TableMetadataParser.read(io, metadataFileLocation);
    return this.current;
  }

  protected String tableMetadataLocation(org.apache.amoro.server.table.TableMetadata tableMeta) {
    return tableMeta.getProperties().get(InternalTableConstants.PROPERTIES_METADATA_LOCATION);
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    Preconditions.checkArgument(
        base != null, "Invalid table metadata for create transaction, base is null");

    Preconditions.checkArgument(
        metadata != null, "Invalid table metadata for create transaction, new metadata is null");
    if (base != current()) {
      throw new CommitFailedException("Cannot commit: stale table metadata");
    }

    String newMetadataFileLocation = InternalTableUtil.genNewMetadataFileLocation(base, metadata);

    try {
      commitTableInternal(tableMetadata, base, metadata, newMetadataFileLocation);
      org.apache.amoro.server.table.TableMetadata updatedMetadata = doCommit();
      checkCommitSuccess(updatedMetadata, newMetadataFileLocation);
    } catch (Exception e) {
      io.deleteFile(newMetadataFileLocation);
    } finally {
      this.tableMetadata = null;
    }
    deleteRemovedMetadataFiles(base, metadata);
    refresh();
  }

  /**
   * Deletes the oldest metadata files if {@link
   * TableProperties#METADATA_DELETE_AFTER_COMMIT_ENABLED} is true.
   *
   * @param base table metadata on which previous versions were based
   * @param metadata new table metadata with updated previous versions
   */
  private void deleteRemovedMetadataFiles(TableMetadata base, TableMetadata metadata) {
    if (base == null) {
      return;
    }

    boolean deleteAfterCommit =
            metadata.propertyAsBoolean(
                    TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED,
                    TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT);

    if (deleteAfterCommit) {
      Set<TableMetadata.MetadataLogEntry> removedPreviousMetadataFiles =
              Sets.newHashSet(base.previousFiles());
      // TableMetadata#addPreviousFile builds up the metadata log and uses
      // TableProperties.METADATA_PREVIOUS_VERSIONS_MAX to determine how many files should stay in
      // the log, thus we don't include metadata.previousFiles() for deletion - everything else can
      // be removed
      metadata.previousFiles().forEach(removedPreviousMetadataFiles::remove);
      if (io() instanceof SupportsBulkOperations) {
        ((SupportsBulkOperations) io())
                .deleteFiles(
                        Iterables.transform(
                                removedPreviousMetadataFiles, TableMetadata.MetadataLogEntry::file));
      } else {
        Tasks.foreach(removedPreviousMetadataFiles)
                .noRetry()
                .suppressFailureWhenFinished()
                .onFailure(
                        (previousMetadataFile, exc) ->
                                LOG.warn(
                                        "Delete failed for previous metadata file: {}", previousMetadataFile, exc))
                .run(previousMetadataFile -> io().deleteFile(previousMetadataFile.file()));
      }
    }
  }

  @Override
  public FileIO io() {
    return this.io;
  }

  @Override
  public String metadataFileLocation(String fileName) {
    return InternalTableUtil.genMetadataFileLocation(current(), fileName);
  }

  @Override
  public LocationProvider locationProvider() {
    return LocationProviders.locationsFor(current().location(), current().properties());
  }

  @Override
  public TableOperations temp(TableMetadata uncommittedMetadata) {
    return TableOperations.super.temp(uncommittedMetadata);
  }

  private org.apache.amoro.server.table.TableMetadata doCommit() {
    ServerTableIdentifier tableIdentifier = tableMetadata.getTableIdentifier();
    AtomicInteger effectRows = new AtomicInteger();
    AtomicReference<org.apache.amoro.server.table.TableMetadata> metadataRef =
        new AtomicReference<>();
    doAsTransaction(
        () -> {
          int effects =
              getAs(
                  TableMetaMapper.class,
                  mapper -> mapper.commitTableChange(tableIdentifier.getId(), tableMetadata));
          effectRows.set(effects);
        },
        () -> {
          org.apache.amoro.server.table.TableMetadata m =
              getAs(
                  TableMetaMapper.class,
                  mapper -> mapper.selectTableMetaById(tableIdentifier.getId()));
          metadataRef.set(m);
        });
    if (effectRows.get() == 0) {
      throw new CommitFailedException(
          "commit failed for version: " + tableMetadata.getMetaVersion() + " has been committed");
    }
    return metadataRef.get();
  }

  /** write iceberg table metadata and apply changes to AMS tableMetadata to commit. */
  private void commitTableInternal(
      org.apache.amoro.server.table.TableMetadata amsTableMetadata,
      TableMetadata base,
      TableMetadata newMetadata,
      String newMetadataFileLocation) {
    if (!Objects.equals(newMetadata.location(), base.location())) {
      throw new UnsupportedOperationException("SetLocation is not supported.");
    }
    OutputFile outputFile = io.newOutputFile(newMetadataFileLocation);
    TableMetadataParser.overwrite(newMetadata, outputFile);

    updateMetadataLocationProperties(
        amsTableMetadata, base.metadataFileLocation(), newMetadataFileLocation);
  }

  protected void updateMetadataLocationProperties(
      org.apache.amoro.server.table.TableMetadata amsTableMetadata,
      String oldMetadataFileLocation,
      String newMetadataFileLocation) {
    Map<String, String> properties = amsTableMetadata.getProperties();
    properties.put(
        InternalTableConstants.PROPERTIES_PREV_METADATA_LOCATION, oldMetadataFileLocation);
    properties.put(InternalTableConstants.PROPERTIES_METADATA_LOCATION, newMetadataFileLocation);
    amsTableMetadata.setProperties(properties);
  }

  private void checkCommitSuccess(
      org.apache.amoro.server.table.TableMetadata updatedTableMetadata,
      String metadataFileLocation) {
    String metaLocationInDatabase = tableMetadataLocation(updatedTableMetadata);
    if (!Objects.equals(metaLocationInDatabase, metadataFileLocation)) {
      throw new CommitFailedException(
          "commit conflict, some other commit happened during this commit. ");
    }
  }
}
