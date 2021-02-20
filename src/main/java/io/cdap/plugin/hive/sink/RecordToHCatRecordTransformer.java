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

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.hive.common.HiveSchemaConverter;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A transform to convert a {@link StructuredRecord} to Hive's {@link HCatRecord}.
 */
public class RecordToHCatRecordTransformer {

  private final HCatSchema hCatSchema;
  private final Schema schema;

  /**
   * A transform to convert a {@link StructuredRecord} to Hive's {@link HCatRecord}. The given {@link Schema} and
   * {@link HCatSchema} must be compatible. To convert one schema to another and supported types
   * see {@link HiveSchemaConverter}
   */
  public RecordToHCatRecordTransformer(HCatSchema hCatSchema, Schema schema) {
    this.hCatSchema = hCatSchema;
    this.schema = schema;
  }

  /**
   * Converts a {@link StructuredRecord} to {@link HCatRecord} using the {@link #hCatSchema}.
   *
   * @param record {@link StructuredRecord} to be converted
   * @return {@link HCatRecord} for the given {@link StructuredRecord}
   * @throws HCatException if failed to set the field in {@link HCatRecord}
   */
  public HCatRecord toHCatRecord(StructuredRecord record) throws Exception {

    Schema schema = this.schema == null ? record.getSchema() : this.schema;
    HCatRecord hCatRecord = new DefaultHCatRecord(hCatSchema.size());

    for (Schema.Field field : schema.getFields()) {
      Preconditions.checkNotNull(record.getSchema().getField(field.getName()), "Missing schema field '%s' in record " +
        "to be written.", field.getName());

      Schema fieldSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
      Object value = convertToHiveType(record.get(field.getName()), fieldSchema, hCatSchema.get(field.getName()));
      hCatRecord.set(field.getName(), hCatSchema, value);
    }

    return hCatRecord;
  }

  private Object convertToHiveType(Object value, Schema schema, HCatFieldSchema hCatFieldSchema) throws Exception {
    if (value == null) {
      return null;
    }
    schema = schema.isNullable() ? schema.getNonNullable() : schema;
    Schema.Type type = schema.getType();
    Schema.LogicalType logicalType = schema.getLogicalType();

    if (logicalType != null) {
      switch (logicalType) {
        case DECIMAL: {
          BigDecimal bigDecimalValue = getDecimalValue(value, schema);
          return HiveDecimal.create(bigDecimalValue);
        }
        case TIMESTAMP_MICROS: {
          ZonedDateTime zonedDateTime = getZonedDateTime((Long) value, TimeUnit.MICROSECONDS);
          return Timestamp.valueOf(zonedDateTime.toLocalDateTime());
        }
        case TIMESTAMP_MILLIS: {
          ZonedDateTime zonedDateTime = getZonedDateTime((Long) value, TimeUnit.MILLISECONDS);
          return Timestamp.valueOf(zonedDateTime.toLocalDateTime());
        }
        default:
          throw new Exception(String.format("Cannot convert '%s' to '%s'.", schema.getType(), hCatFieldSchema));
      }
    }


    switch (type) {
      case ARRAY: {
        HCatFieldSchema subSchema = HiveSchemaConverter.extractSubsSchemaForComplexType(
          hCatFieldSchema.getArrayElementSchema());
        List hCatArrayValues = new ArrayList();
        for (Object val : (List) value) {
          Object hiveValue = convertToHiveType(val, schema.getComponentSchema(), subSchema);
          hCatArrayValues.add(hiveValue);
        }
        return hCatArrayValues;
      }
      case RECORD: {
        List hCatStructValues = new ArrayList();
        StructuredRecord record = (StructuredRecord) value;
        List<Schema.Field> fields = schema.getFields();

        for (Schema.Field field : fields) {
          Object recordElement = record.get(field.getName());
          HCatFieldSchema hCatSubFieldSchema = hCatFieldSchema.getStructSubSchema().get(field.getName());
          Object hCatStructValue = convertToHiveType(recordElement, field.getSchema(), hCatSubFieldSchema);
          hCatStructValues.add(hCatStructValue);
        }
        return hCatStructValues;
      }
      case MAP: {
        Map<Object, Object> hCatMap = new HashMap<>();
        Map<Object, Object> mapValue = (Map<Object, Object>) value;
        Map.Entry<Schema, Schema> mapSchema = schema.getMapSchema();

        PrimitiveTypeInfo mapKeyTypeInfo = hCatFieldSchema.getMapKeyTypeInfo();
        HCatFieldSchema hCatFieldKeySchema = new HCatFieldSchema(null, mapKeyTypeInfo, null);

        HCatFieldSchema hCatFieldValueSchema = HiveSchemaConverter.extractSubsSchemaForComplexType(
          hCatFieldSchema.getMapValueSchema());

        for (Map.Entry<Object, Object> entry : mapValue.entrySet()) {
          Object hCatMapKey = convertToHiveType(entry.getKey(), mapSchema.getKey(), hCatFieldKeySchema);
          Object hCatMapValue = convertToHiveType(entry.getValue(), mapSchema.getValue(), hCatFieldValueSchema);
          hCatMap.put(hCatMapKey, hCatMapValue);
        }

        return hCatMap;
      }
      case INT: {
        PrimitiveTypeInfo typeInfo = hCatFieldSchema.getTypeInfo();
        Integer integerValue = (Integer) value;
        if (typeInfo.equals(TypeInfoFactory.byteTypeInfo)) {
          return integerValue.byteValue();
        } else if (typeInfo.equals(TypeInfoFactory.shortTypeInfo)) {
          return integerValue.shortValue();
        }
        return integerValue;
      }
      case STRING: {
        String stringValue = (String) value;
        PrimitiveTypeInfo primitiveTypeInfo = TypeInfoFactory.getPrimitiveTypeInfo(hCatFieldSchema.getTypeString());
        PrimitiveObjectInspector.PrimitiveCategory primitiveCategory = primitiveTypeInfo.getPrimitiveCategory();
        if (primitiveCategory.equals(PrimitiveObjectInspector.PrimitiveCategory.VARCHAR) ||
          primitiveCategory.equals(PrimitiveObjectInspector.PrimitiveCategory.CHAR)) {
          BaseCharTypeInfo baseCharTypeInfo = (BaseCharTypeInfo) primitiveTypeInfo;
          if (baseCharTypeInfo.getLength() < stringValue.length()) {
            throw new Exception(String.format("Cannot insert record because length exceeds max %d characters.",
                                              baseCharTypeInfo.getLength()));
          }
        }
        return stringValue;
      }
      case BOOLEAN:
      case DOUBLE:
      case FLOAT:
      case LONG:
      case BYTES:
        return value;
      default:
        throw new Exception("");
    }

  }


  private static ZonedDateTime getZonedDateTime(long ts, TimeUnit unit) {
    long mod = unit.convert(1, TimeUnit.SECONDS);
    int fraction = (int) (ts % mod);
    long tsInSeconds = unit.toSeconds(ts);
    // create an Instant with time in seconds and fraction which will be stored as nano seconds.
    Instant instant = Instant.ofEpochSecond(tsInSeconds, unit.toNanos(fraction));
    return ZonedDateTime.ofInstant(instant, ZoneId.ofOffset("UTC", ZoneOffset.UTC));
  }

  private static BigDecimal getDecimalValue(Object value, Schema schema) {
    if (value instanceof ByteBuffer) {
      return new BigDecimal(new BigInteger(Bytes.toBytes((ByteBuffer) value)), schema.getScale());
    }
    try {
      return new BigDecimal(new BigInteger((byte[]) value), schema.getScale());
    } catch (ClassCastException e) {
      throw new ClassCastException(String.format("Field is expected to be a decimal, but is a %s.",
                                                 value.getClass().getSimpleName()));
    }
  }

}
