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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.hive.common.HiveConfig;
import io.cdap.plugin.hive.common.HiveMetastoreUtils;
import io.cdap.plugin.hive.common.HiveSchemaConverter;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;

/**
 * Hive Batch Sink that writes records to external Hive tables.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("Hive")
@Description("Hive Batch Sink that writes records to external Hive tables.")
public class HiveBatchSink extends ReferenceBatchSink<StructuredRecord, NullWritable, HCatRecord> {
  private HiveConfig config;
  private RecordToHCatRecordTransformer recordToHCatRecordTransformer;
  private static final String SINK_TABLE_SCHEMA = "_hive_sink_table_schema_%s";

  public HiveBatchSink(HiveConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(failureCollector);

    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (inputSchema != null && config.canConnect()) {
      try {
        Map<String, String> connectionProperties = config.getConnectionProperties(failureCollector);
        HiveMetaStoreClient hiveMetaClient = HiveMetastoreUtils.getHiveMetaClient(config, connectionProperties);
        List<FieldSchema> fieldSchemas = hiveMetaClient.getSchema(config.getDatabase(), config.getTable());
        HCatSchema hCatSchema = HCatSchemaUtils.getHCatSchema(fieldSchemas);
        HiveSchemaConverter.toHiveSchema(inputSchema, hCatSchema);
      } catch (TException e) {
        failureCollector.addFailure(e.getMessage(), "Check connection properties.");
      } catch (HCatException | IllegalArgumentException e) {
        failureCollector.addFailure(e.getMessage(), null);
      }
      failureCollector.getOrThrowException();
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    Job job = JobUtils.createInstance();
    HiveSinkOutputFormatProvider sinkOutputFormatProvider = new HiveSinkOutputFormatProvider(
      job, config, context.getFailureCollector());

    HCatSchema hCatSchema = sinkOutputFormatProvider.getHiveSchema();

    recordLineage(context, config.referenceName, context.getInputSchema(), "Write", "Write to hive.");

    context.getArguments().set(String.format(SINK_TABLE_SCHEMA, config.getTable()), hCatSchema.getSchemaAsTypeString());
    context.addOutput(Output.of(config.referenceName, sinkOutputFormatProvider));
  }

  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    String schemaString = context.getArguments().get(String.format(SINK_TABLE_SCHEMA, config.getTable()));
    HCatSchema hCatSchema = HCatSchemaUtils.getHCatSchema(schemaString);

    Schema inputSchema = context.getInputSchema();
    if (inputSchema != null) {
      HiveSchemaConverter.toHiveSchema(inputSchema, hCatSchema);
    }
    recordToHCatRecordTransformer = new RecordToHCatRecordTransformer(hCatSchema, inputSchema);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, HCatRecord>> emitter) throws Exception {
    HCatRecord hCatRecord = recordToHCatRecordTransformer.toHCatRecord(input);
    emitter.emit(new KeyValue<>(NullWritable.get(), hCatRecord));
  }
}
