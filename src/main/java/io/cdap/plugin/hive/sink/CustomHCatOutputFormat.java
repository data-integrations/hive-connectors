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

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;

import java.io.IOException;

/** The OutputFormat to use to write data to HCatalog. The key value is ignored and
 *  should be given as null. The value is the HCatRecord to write.*/
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CustomHCatOutputFormat extends HCatOutputFormat {

  @Override
  public RecordWriter<WritableComparable<?>, HCatRecord> getRecordWriter(TaskAttemptContext context) throws IOException,
          InterruptedException {
    //fix class cast exception org.apache.hadoop.hive.ql.log.PerfLogger to org.apache.hadoop.hive.ql.log.PerfLogger
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

    RecordWriter<WritableComparable<?>, HCatRecord> recordWriter;

    try {
      recordWriter = super.getRecordWriter(context);
    } finally {
      Thread.currentThread().setContextClassLoader(classLoader);
    }
    return recordWriter;
  }
}
