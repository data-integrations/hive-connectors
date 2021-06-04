/*
 * Copyright © 2021 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.plugin.hive;

import io.cdap.cdap.api.macro.Macros;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.hive.common.HiveConfig;
import io.cdap.plugin.hive.common.HiveMetastoreUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.mockito.MockedStatic;
import org.mockito.internal.util.reflection.FieldSetter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;


/**
 * Tests for hive plugin
 */
public class HiveTest {
    MockedStatic<HiveMetastoreUtils> hiveMetastoreUtilsMockedStatic;

    @Before
    public void init() {
        if (hiveMetastoreUtilsMockedStatic == null) {
            hiveMetastoreUtilsMockedStatic = mockStatic(HiveMetastoreUtils.class);
        }
    }

    @After
    public void close() {
        if (hiveMetastoreUtilsMockedStatic != null) {
            hiveMetastoreUtilsMockedStatic.close();
        }
    }


    private HiveConfig getConfig() throws NoSuchFieldException {
        HiveConfig hiveConfig = new HiveConfig();
        FieldSetter
                .setField(hiveConfig, HiveConfig.class.getDeclaredField("metastoreURL"),
                        "thrift://somehost.net:9083");
        FieldSetter
                .setField(hiveConfig, HiveConfig.class.getDeclaredField("database"), "dbewdbv");
        FieldSetter
                .setField(hiveConfig, HiveConfig.class.getDeclaredField("table"), "tb");
        FieldSetter
                .setField(hiveConfig, HiveConfig.class.getDeclaredField("partitionFilter"), "pf");
        FieldSetter
                .setField(hiveConfig, HiveConfig.class.getDeclaredField("connectionProperties"), "k=v");

        return hiveConfig;
    }


    @Test
    public void testValidConfig() throws Exception {
        HiveConfig config = getConfig();
        MockFailureCollector collector = new MockFailureCollector("hivesink");

        HiveMetaStoreClient hiveMetaClient = mock(HiveMetaStoreClient.class);
        when(hiveMetaClient.getDatabase(config.getDatabase())).thenReturn(new Database());
        when(hiveMetaClient.tableExists(config.getDatabase(), config.getTable())).thenReturn(true);


        hiveMetastoreUtilsMockedStatic.when(() -> HiveMetastoreUtils.
                getHiveMetaClient(config, config.getConnectionProperties(new MockFailureCollector())))
                .thenReturn(hiveMetaClient);

        config.validate(collector);
        Assert.assertTrue(collector.getValidationFailures().isEmpty());

    }

    @Test
    public void testInvalidDatabase() throws Exception {
        HiveConfig config = getConfig();

        HiveMetaStoreClient hiveMetaClient = mock(HiveMetaStoreClient.class);
        when(hiveMetaClient.getDatabase(config.getDatabase())).thenThrow(new TException());

        hiveMetastoreUtilsMockedStatic.when(() -> HiveMetastoreUtils
                .getHiveMetaClient(config, config.getConnectionProperties(new MockFailureCollector())))
                .thenReturn(hiveMetaClient);

        validateConfigValidationFail(config, HiveConfig.NAME_DATABASE);

    }

    @Test
    public void testInvalidTable() throws Exception {
        HiveConfig config = getConfig();

        HiveMetaStoreClient hiveMetaClient = mock(HiveMetaStoreClient.class);
        when(hiveMetaClient.getDatabase(config.getDatabase())).thenReturn(new Database());
        when(hiveMetaClient.tableExists(config.getDatabase(), config.getTable())).thenReturn(false);

        hiveMetastoreUtilsMockedStatic.when(() -> HiveMetastoreUtils.
                getHiveMetaClient(config, config.getConnectionProperties(new MockFailureCollector())))
                .thenReturn(hiveMetaClient);

        validateConfigValidationFail(config, HiveConfig.NAME_TABLE);
    }

    @Test
    public void testInvalidProp() throws Exception {
        HiveConfig config = getConfig();

        HiveMetaStoreClient hiveMetaClient = mock(HiveMetaStoreClient.class);
        FieldSetter
                .setField(config, HiveConfig.class.getDeclaredField("connectionProperties"), "k,v");

        hiveMetastoreUtilsMockedStatic.when(() -> HiveMetastoreUtils.
                getHiveMetaClient(config, config.getConnectionProperties(new MockFailureCollector())))
                .thenReturn(hiveMetaClient);

        validateConfigValidationFail(config, HiveConfig.NAME_CONNECTION_PROPERTIES);
    }

    @Test
    public void testInvalidURL() throws Exception {
        HiveConfig config = getConfig();
        HiveMetaStoreClient hiveMetaClient = null;

        hiveMetastoreUtilsMockedStatic.when(() -> HiveMetastoreUtils.
                getHiveMetaClient(config, config.getConnectionProperties(new MockFailureCollector())))
                .thenReturn(hiveMetaClient);

        validateConfigValidationFail(config, HiveConfig.NAME_METASTORE_URL);
    }

    @Test
    public void testValidConfigWithMacroURL() throws Exception {
        MockFailureCollector collector = new MockFailureCollector();
        HiveConfig config = getMacroHiveConfig(HiveConfig.NAME_METASTORE_URL, "metastoreurl");

        config.validate(collector);
        Assert.assertTrue(collector.getValidationFailures().isEmpty());
    }

    @Test
    public void testValidConfigWithMacroProp() throws Exception {
        MockFailureCollector collector = new MockFailureCollector();
        HiveConfig config = getMacroHiveConfig(HiveConfig.NAME_CONNECTION_PROPERTIES, "metastoreurl");

        config.validate(collector);
        Assert.assertTrue(collector.getValidationFailures().isEmpty());
    }

    @Test
    public void testValidConfigWithMacroDB() throws Exception {
        MockFailureCollector collector = new MockFailureCollector();
        HiveConfig config = getMacroHiveConfig(HiveConfig.NAME_DATABASE, "metastoreurl");
        FieldSetter
                .setField(config, HiveConfig.class.getDeclaredField("metastoreURL"),
                        "thrift://somehost.net:9083");
        HiveMetaStoreClient hiveMetaClient = mock(HiveMetaStoreClient.class);

        hiveMetastoreUtilsMockedStatic.when(() -> HiveMetastoreUtils.
                getHiveMetaClient(config, config.getConnectionProperties(new MockFailureCollector())))
                .thenReturn(hiveMetaClient);

        config.validate(collector);
        Assert.assertTrue(collector.getValidationFailures().isEmpty());
    }

    @Test
    public void testValidConfigWithMacroTable() throws Exception {
        MockFailureCollector collector = new MockFailureCollector();
        HiveConfig config = getMacroHiveConfig(HiveConfig.NAME_TABLE, "metastoreurl");
        FieldSetter
                .setField(config, HiveConfig.class.getDeclaredField("metastoreURL"),
                        "thrift://somehost.net:9083");
        FieldSetter
                .setField(config, HiveConfig.class.getDeclaredField("database"),
                        "default");
        HiveMetaStoreClient hiveMetaClient = mock(HiveMetaStoreClient.class);
        when(hiveMetaClient.getDatabase(config.getDatabase())).thenReturn(new Database());

        hiveMetastoreUtilsMockedStatic.when(() -> HiveMetastoreUtils.
                getHiveMetaClient(config, config.getConnectionProperties(new MockFailureCollector())))
                .thenReturn(hiveMetaClient);

        config.validate(collector);
        Assert.assertTrue(collector.getValidationFailures().isEmpty());
    }


    private static void validateConfigValidationFail(HiveConfig config, String propertyValue) {
        MockFailureCollector collector = new MockFailureCollector();
        ValidationFailure failure;
        try {
            config.validate(collector);
            Assert.assertEquals(1, collector.getValidationFailures().size());
            failure = collector.getValidationFailures().get(0);
        } catch (ValidationException e) {
            // it is possible that validation exception was thrown during validation. so catch the exception
            Assert.assertEquals(1, e.getFailures().size());
            failure = e.getFailures().get(0);
        }

        Assert.assertEquals(propertyValue, failure.getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
    }

    private static HiveConfig getMacroHiveConfig(String macroField, String macroValue) throws NoSuchFieldException {
        HiveConfig config = new HiveConfig();
        Set<String> macroFields = new HashSet<>();
        macroFields.add(macroField);
        Map<String, String> properties = new HashMap<>();
        properties.put(macroField, macroValue);
        Macros macros = new Macros(macroFields, null);

        PluginProperties rawProperties = PluginProperties.builder()
                .addAll(properties)
                .build()
                .setMacros(macros);

        FieldSetter.setField(config, HiveConfig.class.getSuperclass().getSuperclass().getDeclaredField("rawProperties"),
                rawProperties);
        FieldSetter.setField(config, HiveConfig.class.getSuperclass().getSuperclass().getDeclaredField("macroFields"),
                macroFields);

        return config;
    }
}
