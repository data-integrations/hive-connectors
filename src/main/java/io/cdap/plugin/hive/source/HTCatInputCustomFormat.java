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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

import java.io.IOException;

/**
 *
 */
public class HTCatInputCustomFormat extends HCatInputFormat {

  @Override
  public RecordReader<WritableComparable, HCatRecord> createRecordReader
          (InputSplit split, TaskAttemptContext taskContext) throws IOException, InterruptedException {
    //fix class cast exception org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe cannot be cast to
    // org.apache.hadoop.hive.serde2.Deserializer. Please
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    RecordReader<WritableComparable, HCatRecord> recordReader;
    try {
      recordReader = super.createRecordReader(split, taskContext);
    } finally {
      Thread.currentThread().setContextClassLoader(classLoader);
    }
    return recordReader;
  }
}
