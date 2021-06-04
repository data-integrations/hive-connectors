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

package io.cdap.plugin.hive.source;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSource;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.hive.common.HiveConfig;
import io.cdap.plugin.hive.common.HiveMetastoreUtils;
import io.cdap.plugin.hive.common.HiveSchemaConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Hive Batch Source to read records from external Hive tables.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("Hive")
@Description("Hive Batch Source to read records from external Hive tables.")
public class HiveBatchSource extends ReferenceBatchSource<WritableComparable, Object, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(HiveBatchSource.class);
  private static final String SOURCE_TABLE_SCHEMA = "_hive_source_table_schema_%s";
  private HiveConfig config;
  private HCatRecordTransformer hCatRecordTransformer;

  public HiveBatchSource(HiveConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();
    config.validate(collector);

    setOutputSchema(pipelineConfigurer, collector);
    collector.getOrThrowException();
  }

  private void setOutputSchema(PipelineConfigurer pipelineConfigurer, FailureCollector collector) {
    try {
      if (!config.canConnect()) {
        return;
      }
      HiveMetaStoreClient hiveMetaClient =
        HiveMetastoreUtils.getHiveMetaClient(config, config.getConnectionProperties(collector));
      List<FieldSchema> schemaFields = hiveMetaClient.getSchema(config.getDatabase(), config.getTable());
      HCatSchema hCatSchema = HCatSchemaUtils.getHCatSchema(schemaFields);
      Schema schema = HiveSchemaConverter.toSchema(hCatSchema);
      pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
    } catch (TException | HCatException e) {
      collector.addFailure("Unable to connect to the Metastore.", "Check connection requirements.");
      LOG.error("Unable to connect to the HiveMetastore.", e);
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    FailureCollector failureCollector = context.getFailureCollector();
    config.validate(failureCollector);

    Map<String, String> connectionProperties = config.getConnectionProperties(failureCollector);
    failureCollector.getOrThrowException();

    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();
    conf.set(HiveConf.ConfVars.METASTOREURIS.varname, config.getMetastoreUrl());

    connectionProperties.forEach(conf::set);

    HCatInputFormat.setInput(conf, config.getDatabase(), config.getTable(), config.getPartitionFilter());
    HCatSchema hCatSchema = HCatInputFormat.getTableSchema(conf);
    HCatInputFormat.setOutputSchema(job, hCatSchema);

    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    lineageRecorder.createExternalDataset(HiveSchemaConverter.toSchema(hCatSchema));

    context.getArguments().set(String.format(SOURCE_TABLE_SCHEMA, config.getTable()),
                               hCatSchema.getSchemaAsTypeString());
    context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(HTCatInputCustomFormat.class, conf)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    String schemaString = context.getArguments().get(String.format(SOURCE_TABLE_SCHEMA, config.getTable()));
    HCatSchema hCatSchema = HCatSchemaUtils.getHCatSchema(schemaString);
    Schema schema = HiveSchemaConverter.toSchema(hCatSchema);
    hCatRecordTransformer = new HCatRecordTransformer(hCatSchema, schema);
  }

  @Override
  public void transform(KeyValue<WritableComparable, Object> input, Emitter<StructuredRecord> emitter)
    throws Exception {
    StructuredRecord record = hCatRecordTransformer.toRecord((HCatRecord) input.getValue());
    emitter.emit(record);
  }
}
