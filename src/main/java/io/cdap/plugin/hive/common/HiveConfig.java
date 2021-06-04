/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.hive.common;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.KeyValueListParser;
import io.cdap.plugin.common.ReferencePluginConfig;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.thrift.TException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Holds configuration necessary for {@link io.cdap.plugin.hive.source.HiveBatchSource} and
 * {@link io.cdap.plugin.hive.sink.HiveBatchSink}
 */
public class HiveConfig extends ReferencePluginConfig {

  // properties
  public static final String NAME_METASTORE_URL = "metastoreUrl";
  public static final String NAME_DATABASE = "database";
  public static final String NAME_TABLE = "table";
  public static final String NAME_PARTITION_FILTER = "partitionFilter";
  public static final String NAME_CONNECTION_PROPERTIES = "connectionProperties";

  // description
  public static final String DESC_METASTORE_URL = "URL to connect to Hive Metastore. Following metastore are " +
    "supported for Hive: MysQL,Oracle, Postgres and SQL Server. Format: thrift://<hostname>:<port>.";
  public static final String DESC_DATABASE = "Database to connect to.";
  public static final String DESC_TABLE = "Table to read from.";
  public static final String DESC_PARTITION_FILTER = "A filter expression to filter the data. Filters " +
    "must be specified only on partition columns. Refer to https://cwiki.apache.org/confluence/display/" +
    "Hive/Partition+Filter+Syntax for filter syntax.";
  public static final String DESC_CONNECTION_PROPERTIES = "Properties for the Hive Metastore connection.";

  @Name(NAME_METASTORE_URL)
  @Description(DESC_METASTORE_URL)
  @Macro
  private String metastoreURL;

  @Name(NAME_DATABASE)
  @Description(DESC_DATABASE)
  @Macro
  private String database;

  @Name(NAME_TABLE)
  @Description(DESC_TABLE)
  @Macro
  private String table;

  @Name(NAME_PARTITION_FILTER)
  @Description(DESC_PARTITION_FILTER)
  @Nullable
  @Macro
  private String partitionFilter;

  @Name(NAME_CONNECTION_PROPERTIES)
  @Description(DESC_CONNECTION_PROPERTIES)
  @Nullable
  @Macro
  private String connectionProperties;

  public HiveConfig() {
    super("Hive");
  }

  public String getMetastoreUrl() {
    return metastoreURL;
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  @Nullable
  public String getPartitionFilter() {
    return partitionFilter;
  }

  public void validate(FailureCollector collector) {
    if (containsMacro(NAME_METASTORE_URL) || containsMacro(NAME_CONNECTION_PROPERTIES)) {
      return;
    }
    Map<String, String> connectionProperties = getConnectionProperties(collector);
    collector.getOrThrowException();

    HiveMetaStoreClient client = validateMetastoreUrl(collector, connectionProperties);
    collector.getOrThrowException();

    try {
      if (containsMacro(NAME_DATABASE)) {
        return;
      }
      validateDatabase(client, collector);
      collector.getOrThrowException();

      if (containsMacro(NAME_TABLE)) {
        return;
      }
      validateTable(client, collector);
      collector.getOrThrowException();
    } finally {
      HCatUtil.closeHiveClientQuietly(client);
    }

  }

  private HiveMetaStoreClient validateMetastoreUrl(FailureCollector collector,
                                                   Map<String, String> connectionProperties) {
    if (Strings.isNullOrEmpty(metastoreURL)) {
      collector.addFailure(String.format("Parameter \"%s\" is mandatory.", NAME_METASTORE_URL),
                           "Please provide a value.")
        .withConfigProperty(NAME_METASTORE_URL);
      return null;
    }

    try {
      new URI(metastoreURL);
    } catch (URISyntaxException e) {
      collector.addFailure("Provided Metastore URL is invalid.",
                           "Please provide a correct URL. Format: thrift://<hostname>:<port>.")
        .withConfigProperty(NAME_METASTORE_URL);
      return null;
    }
    HiveMetaStoreClient hiveMetaClient = HiveMetastoreUtils.getHiveMetaClient(this, connectionProperties);
    if (hiveMetaClient == null) {
      collector.addFailure("Something went wrong while connecting to the Metastore.",
                           "Please re-check the provided URL.")
        .withConfigProperty(NAME_METASTORE_URL);
    }
    return hiveMetaClient;
  }

  private void validateDatabase(HiveMetaStoreClient client, FailureCollector collector) {
    if (Strings.isNullOrEmpty(database)) {
      collector.addFailure(String.format("Parameter \"%s\" is mandatory.", NAME_DATABASE),
                           "Please provide a value.")
        .withConfigProperty(NAME_DATABASE);
      return;
    }

    try {
      client.getDatabase(getDatabase());
    } catch (TException e) {
      collector.addFailure("Provided database doesn't exist.",
                           "Please provide an existing database name.")
        .withConfigProperty(NAME_DATABASE);
    }
  }

  private void validateTable(HiveMetaStoreClient client, FailureCollector collector) {
    if (Strings.isNullOrEmpty(table)) {
      collector.addFailure(String.format("Parameter \"%s\" is mandatory.", NAME_TABLE),
                           "Please provide a value.")
        .withConfigProperty(NAME_TABLE);
      return;
    }
    try {
      if (!client.tableExists(getDatabase(), getTable())) {
        collector.addFailure("Provided table doesn't exist.",
                             "Please provide an existing table name.")
          .withConfigProperty(NAME_TABLE);
      }
    } catch (TException e) {
      collector.addFailure("Something went wrong while validating the table.",
                           "Please re-check table name.")
        .withConfigProperty(NAME_TABLE);
    }
  }

  public Map<String, String> getConnectionProperties(FailureCollector failureCollector) {
    if (containsMacro(NAME_CONNECTION_PROPERTIES) || Strings.isNullOrEmpty(connectionProperties)) {
      return Collections.emptyMap();
    }
    Map<String, String> properties = new HashMap<>();
    KeyValueListParser kvParser = new KeyValueListParser(";", "=");
    try {
      for (KeyValue<String, String> alias : kvParser.parse(connectionProperties)) {
        properties.put(alias.getKey(), alias.getValue());
      }
    } catch (Exception e) {
      failureCollector.addFailure("Invalid syntax for key-value pair.",
                                  "Pair delimiter should be a colon and key-value " +
                                    "delimiter should be an equal sign. Example: key1=value1;key2=value2")
        .withConfigProperty(NAME_CONNECTION_PROPERTIES);

    }
    return properties;
  }

  public boolean canConnect() {
    return !(containsMacro(NAME_METASTORE_URL) || containsMacro(NAME_CONNECTION_PROPERTIES)
      || containsMacro(NAME_DATABASE) || containsMacro(NAME_TABLE));
  }
}
