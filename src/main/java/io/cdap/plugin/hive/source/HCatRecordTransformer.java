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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.hive.common.HiveSchemaConverter;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A transform to convert a {@link HCatRecord} from hive to {@link StructuredRecord}.
 */
public class HCatRecordTransformer {
  private final HCatSchema hCatSchema;
  private final Schema schema;

  /**
   * A transform to convert a {@link HCatRecord} to Hive's {@link StructuredRecord}. The given {@link Schema} and
   * {@link HCatSchema} must be compatible. To convert one schema to another and supported types
   * see {@link HiveSchemaConverter}
   */
  public HCatRecordTransformer(HCatSchema hCatSchema, Schema schema) {
    this.hCatSchema = hCatSchema;
    this.schema = schema;
  }

  /**
   * Converts a {@link HCatRecord} read from a hive table to {@link StructuredRecord} using the {@link Schema} created
   * from the {@link HCatSchema}.
   *
   * @param hCatRecord the record
   * @return the converted {@link StructuredRecord}
   */
  public StructuredRecord toRecord(HCatRecord hCatRecord) throws Exception {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      Schema fieldSchema = field.getSchema();

      if (fieldSchema.isNullable()) {
        fieldSchema = fieldSchema.getNonNullable();
      }

      Schema.LogicalType logicalType = fieldSchema.getLogicalType();
      if (logicalType != null) {
        switch (logicalType) {
          case TIMESTAMP_MICROS:
          case TIMESTAMP_MILLIS:
            Timestamp timestamp = hCatRecord.getTimestamp(fieldName, hCatSchema);
            Instant instant = timestamp.toInstant();
            ZonedDateTime utc = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"));
            builder.setTimestamp(fieldName, utc);
            break;
          case DECIMAL:
            BigDecimal decimal = hCatRecord.getDecimal(fieldName, hCatSchema).bigDecimalValue();
            builder.setDecimal(fieldName, decimal);
            break;
          default:
            throw new RuntimeException(String.format("Logical type '%s' is not supported", logicalType));
        }
        continue;
      }

      Schema.Type type = fieldSchema.getType();
      if (!type.isSimpleType()) {
        switch (type) {
          case ARRAY:
            Schema componentSchema = fieldSchema.getComponentSchema();
            List arrayValues = (List) getSchemaCompatibleValue(hCatRecord, fieldName);
            List values = convertArray(arrayValues, componentSchema);
            builder.set(fieldName, values);
            break;
          case MAP:
            Map map = (Map) getSchemaCompatibleValue(hCatRecord, fieldName);
            Map mapValues = convertMap(map, fieldSchema.getMapSchema());
            builder.set(fieldName, mapValues);
            break;
          case RECORD: {
            List listValue = (List) getSchemaCompatibleValue(hCatRecord, fieldName);
            StructuredRecord structuredRecord = convertRecord(listValue, fieldSchema);
            builder.set(fieldName, structuredRecord);
            break;
          }
        }
      } else {
        switch (type) {
          case BOOLEAN:
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE:
          case STRING:
          case BYTES: {
            try {
              builder.set(fieldName, getSchemaCompatibleValue(hCatRecord, fieldName));
              break;
            } catch (Throwable t) {
              throw new RuntimeException(String.format("Error converting field '%s' of type %s",
                                                       fieldName, type), t);
            }
          }
          default: {
            throw new IllegalStateException(String.format("Output fieldSchema contains field '%s' with unsupported " +
                                                            "type %s.", fieldName, type));
          }
        }
      }
    }
    return builder.build();
  }

  private StructuredRecord convertRecord(List listValue, Schema schema) throws Exception {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    for (int i = 0; i < schema.getFields().size(); i++) {
      Object o = listValue.get(i);
      Schema.Field field = schema.getFields().get(i);
      Object o1 = convertType(o, field.getSchema());
      builder.set(field.getName(), o1);
    }
    return builder.build();
  }

  private List<?> convertArray(List values, Schema componentSchema) throws Exception {
    List<Object> newValues = new ArrayList();
    for (Object value : values) {
      newValues.add(convertType(value, componentSchema));
    }
    return newValues;
  }


  private Object convertType(Object value, Schema cdapSchema) throws Exception {
    Schema nonNullableSchema = cdapSchema.isNullable() ? cdapSchema.getNonNullable() : cdapSchema;
    Schema.Type type = nonNullableSchema.getType();

    if (nonNullableSchema.isSimpleOrNullableSimple()) {
      return convertPrimitiveType(value, nonNullableSchema);
    }

    switch (type) {
      case RECORD:
        return convertRecord((List) value, nonNullableSchema);
      case MAP:
        return convertMap((Map) value, nonNullableSchema.getMapSchema());
      case ARRAY:
        return convertArray((List) value, nonNullableSchema.getComponentSchema());
      default:
        throw new Exception(String.format("Type '%s' is not supported.", type));
    }
  }

  /**
   * Converts primitive to CDAP compatible type
   *
   * @param value  field value
   * @param schema field schema
   * @return structured record compatible field
   */
  private Object convertPrimitiveType(Object value, Schema schema) {
    Schema.Type type = schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();
    Schema.LogicalType logicalType = schema.getLogicalType();

    if (logicalType != null) {
      switch (logicalType) {
        case DECIMAL:
          HiveDecimal hiveDecimal = (HiveDecimal) value;
          return hiveDecimal == null ? null : hiveDecimal.bigDecimalValue();
        case TIMESTAMP_MILLIS:
          Timestamp timestamp = (Timestamp) value;
          return timestamp == null ? null : timestamp.getTime();
      }
    }

    switch (type) {
      case BOOLEAN:
      case INT: {
        if (value instanceof Byte) {
          return (int) (Byte) value;
        }
        if (value instanceof Short) {
          return (int) (Short) value;
        }
        return value;
      }
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
        if (value instanceof HiveChar) {
          return value.toString();
        }

        if (value instanceof HiveVarchar) {
          return value.toString();
        }
        return value;
      default:
        throw new IllegalArgumentException(String.format("Field of type '%s' is not supported.", type));
    }
  }

  /**
   * Converts map type to CDAP map type
   *
   * @param map       field values
   * @param mapSchema field schemas
   * @return CDAP compatible map
   * @throws Exception if type is not supported
   */
  private Map convertMap(Map map, Map.Entry<Schema, Schema> mapSchema) throws Exception {

    Object mapKeyValue = map.keySet().iterator().next();
    Object mapValue = map.get(mapKeyValue);

    Object convertedKeyType = convertPrimitiveType(mapKeyValue, mapSchema.getKey());
    Object convertedValueType = convertType(mapValue, mapSchema.getValue());

    return Collections.singletonMap(convertedKeyType, convertedValueType);
  }


  /**
   * Converts the value for a field from {@link HCatRecord} to the compatible {@link Schema} type to be represented in
   * {@link StructuredRecord}. For schema conversion details and supported type see {@link HiveSchemaConverter}.
   *
   * @param hCatRecord the record being converted to {@link StructuredRecord}
   * @param fieldName  name of the field
   * @return the value for the given field which is of type compatible with {@link Schema}.
   * @throws HCatException if failed to get {@link HCatFieldSchema} for the given field
   */
  private Object getSchemaCompatibleValue(HCatRecord hCatRecord, String fieldName) throws HCatException {
    HCatFieldSchema hCatFieldSchema = hCatSchema.get(fieldName);
    if (hCatFieldSchema.isComplex()) {
      HCatFieldSchema.Category category = hCatFieldSchema.getCategory();
      switch (category) {
        case ARRAY:
          return hCatRecord.getList(fieldName, hCatSchema);
        case MAP:
          return hCatRecord.getMap(fieldName, hCatSchema);
        case STRUCT:
          return hCatRecord.getStruct(fieldName, hCatSchema);
        default:
          throw new IllegalArgumentException(
            String.format("Table schema contains field '%s' with unsupported type %s. To read this table you should" +
                            " provide input schema in which this field is dropped.", fieldName, category.name()));

      }

    }
    PrimitiveObjectInspector.PrimitiveCategory category = hCatFieldSchema
      .getTypeInfo().getPrimitiveCategory();

    switch (category) {

      // Its not required to check that the schema has the same type because if the user provided  the Schema then
      // the HCatSchema was obtained through the convertor and if the user didn't the Schema was obtained through the
      // and hence the types will be same.
      case BOOLEAN:
        return hCatRecord.getBoolean(fieldName, hCatSchema);
      case BYTE:
        Byte byteValue = hCatRecord.getByte(fieldName, hCatSchema);
        return byteValue == null ? null : (int) byteValue;
      case SHORT:
        Short shortValue = hCatRecord.getShort(fieldName, hCatSchema);
        return shortValue == null ? null : (int) shortValue;
      case INT:
        return hCatRecord.getInteger(fieldName, hCatSchema);
      case LONG:
        return hCatRecord.getLong(fieldName, hCatSchema);
      case FLOAT:
        return hCatRecord.getFloat(fieldName, hCatSchema);
      case DOUBLE:
        return hCatRecord.getDouble(fieldName, hCatSchema);
      case CHAR:
        HiveChar charValue = hCatRecord.getChar(fieldName, hCatSchema);
        return charValue == null ? null : charValue.toString();
      case STRING:
        return hCatRecord.getString(fieldName, hCatSchema);
      case VARCHAR:
        HiveVarchar varcharValue = hCatRecord.getVarchar(fieldName, hCatSchema);
        return varcharValue == null ? null : varcharValue.toString();
      case BINARY:
        return hCatRecord.getByteArray(fieldName, hCatSchema);
      case TIMESTAMP:
        Timestamp timestamp = hCatRecord.getTimestamp(fieldName, hCatSchema);
        return timestamp.getTime();
      case DECIMAL:
        HiveDecimal decimal = hCatRecord.getDecimal(fieldName, hCatSchema);
        return decimal == null ? null : decimal.bigDecimalValue();

      // We can support VOID by having Schema type as null but HCatRecord does not support VOID and since we read
      // write through HCatSchema and HCatRecord we are not supporting VOID too for consistent behavior.
      case VOID:
      case DATE:
      case UNKNOWN:
      default:
        throw new IllegalArgumentException(String.format("Table schema contains field '%s' with unsupported type %s. " +
                                                           "To read this table you should provide input schema in " +
                                                           "which this field is dropped.", fieldName, category.name()));
    }
  }
}
