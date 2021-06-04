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

package io.cdap.plugin.hive.sink;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.batch.ConfigurationUtils;
import io.cdap.plugin.hive.common.HiveConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * Output format provider for Hive Sink
 */
public class HiveSinkOutputFormatProvider implements OutputFormatProvider {

  private static final Gson GSON = new Gson();
  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() {
  }.getType();

  private final Map<String, String> conf;
  private HCatSchema hiveSchema;

  public HiveSinkOutputFormatProvider(Job job, HiveConfig config, FailureCollector collector) throws IOException {
    Configuration originalConf = job.getConfiguration();
    Configuration modifiedConf = new Configuration(originalConf);
    modifiedConf.set(HiveConf.ConfVars.METASTOREURIS.varname, config.getMetastoreUrl());
    CustomHCatOutputFormat.setOutput(modifiedConf, job.getCredentials(), OutputJobInfo.create(config.getDatabase(),
                                                                                              config.getTable(),
                                                                                              getPartitions(config)));

    Map<String, String> connectionProperties = config.getConnectionProperties(collector);
    collector.getOrThrowException();

    connectionProperties.forEach(modifiedConf::set);

    hiveSchema = CustomHCatOutputFormat.getTableSchema(modifiedConf);

    OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(modifiedConf);
    // if dynamic partitioning was used then append the dynamic partitioning columns to the table schema obtained from
    // hive as the schema obtained does not have these columns. The partition columns which are static
    // does not need to be appended since they are not expected to be present in the incoming record.
    if (jobInfo.isDynamicPartitioningUsed()) {
      HCatSchema partitionColumns = jobInfo.getTableInfo().getPartitionColumns();
      List<String> dynamicPartitioningKeys = jobInfo.getDynamicPartitioningKeys();

      for (String dynamicPartitioningKey : dynamicPartitioningKeys) {
        HCatFieldSchema curFieldSchema = partitionColumns.get(dynamicPartitioningKey);
        hiveSchema.append(curFieldSchema);
      }
    }

    CustomHCatOutputFormat.setSchema(modifiedConf, hiveSchema);
    conf = ConfigurationUtils.getNonDefaultConfigurations(modifiedConf);
  }

  private Map<String, String> getPartitions(HiveConfig config) {
    Map<String, String> partitionValues = null;
    if (config.getPartitionFilter() != null) {
      partitionValues = GSON.fromJson(config.getPartitionFilter(), STRING_MAP_TYPE);
    }
    return partitionValues;
  }

  /**
   * @return the {@link HCatSchema} for the Hive table for this {@link HiveSinkOutputFormatProvider}
   */
  public HCatSchema getHiveSchema() {
    return hiveSchema;
  }

  @Override
  public String getOutputFormatClassName() {
    return CustomHCatOutputFormat.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    return conf;
  }
}
