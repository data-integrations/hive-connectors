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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Hive Metastore Utils
 */
public class HiveMetastoreUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HiveMetastoreUtils.class);

  /**
   * Returns {@link HiveMetaStoreClient} with a connection to a metastore
   * using {@link HiveConfig} that is used to validate properties.
   *
   * @param config               the config that is used for the connection
   * @param connectionProperties
   * @return the client that is of type {@link HiveMetaStoreClient}.
   */
  public static HiveMetaStoreClient getHiveMetaClient(HiveConfig config, Map<String, String> connectionProperties) {
    // fix class cast exception org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl
    // to org.apache.hadoop.hive.metastore.MetaStoreFilterHook
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(HiveMetastoreUtils.class.getClassLoader());
    try {
      HiveConf conf = new HiveConf();
      conf.set(HiveConf.ConfVars.METASTOREURIS.varname, config.getMetastoreUrl());
      // this is only used for validating metastore url, default value is 600s.
      conf.set(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT.varname, "10s");
      connectionProperties.forEach(conf::set);
      return new HiveMetaStoreClient(conf);
    } catch (MetaException e) {
      LOG.error(e.getMessage(), e.getCause());
    } finally {
      Thread.currentThread().setContextClassLoader(classLoader);
    }
    return null;
  }
}
