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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;

import java.util.List;
import java.util.Map;

/**
 * <p>>Hive Schema Converter class to convert {@link Schema} to {@link HCatSchema} and vice versa.</p>
 * Note: If the {@link HCatSchema} contains non-primitive type then this conversion to {@link Schema} will fail.
 */
public class HiveSchemaConverter {

  /**
   * Converts a CDAP's {@link Schema} to Hive's {@link HCatSchema} while verifying the fields in the given
   * {@link Schema} to exists in the table. The valid types for {@link Schema} which can be converted into
   * {@link HCatSchema} are boolean, int, long, float, double, string and bytes.
   *
   * @param schema the {@link Schema}
   * @return {@link HCatSchema} for the given {@link Schema}
   * @throws NullPointerException if a field in the given {@link Schema} is not found in table's {@link HCatSchema}
   */
  public static HCatSchema toHiveSchema(Schema schema, HCatSchema tableSchema) throws HCatException {
    List<HCatFieldSchema> fields = Lists.newArrayList();
    for (Schema.Field field : schema.getFields()) {
      Integer position = tableSchema.getPosition(field.getName());
      if (position == null) {
        throw new IllegalArgumentException(String.format("Missing field `%s` in table schema", field.getName()));
      }
      HCatFieldSchema tableFieldSchema = tableSchema.get(position);

      Preconditions.checkNotNull(tableFieldSchema, "Missing field %s in table schema", field.getName());

      HCatFieldSchema fieldSchema = getType(field.getName(), field.getSchema(), tableFieldSchema);
      fields.add(fieldSchema);
    }
    return new HCatSchema(fields);
  }

  /**
   * <p>Converts a {@link HCatSchema} from hive to {@link Schema} for CDAP.</p>
   * <p><b>Note:</b> This conversion does not support non-primitive types and the conversion will fail.
   * The conversion might also change the primitive type.
   * See {@link #getType(String, PrimitiveTypeInfo)} for details.</p>
   * The valid types of {@link HCatFieldSchema} which can be converted into {@link Schema} are boolean, byte, char,
   * short, int, long, float, double, string, varchar, binary
   *
   * @param hiveSchema the {@link HCatSchema} of the hive table
   * @return {@link Schema} for the given {@link HCatSchema}
   */
  public static Schema toSchema(HCatSchema hiveSchema) throws HCatException {
    return toSchema("record", hiveSchema);
  }

  /**
   * Converts {@link HCatSchema} to {@link Schema}
   *
   * @param recordName field name
   * @param hiveSchema field schema
   * @return {@link Schema} CDAP comaptible schema
   * @throws {@link HCatException} for unknown types
   */
  private static Schema toSchema(String recordName, HCatSchema hiveSchema) throws HCatException {
    List<Schema.Field> fields = Lists.newArrayList();
    for (HCatFieldSchema field : hiveSchema.getFields()) {
      String name = field.getName();
      if (field.isComplex()) {
        fields.add(Schema.Field.of(name, getComplexType(name, field)));
      } else {
        fields.add(Schema.Field.of(name, getType(name, field.getTypeInfo())));
      }
    }
    return Schema.recordOf(recordName, fields);
  }

  /**
   * Returns the {@link Schema.Type} compatible for this field from hive.
   *
   * @param name              name of the field
   * @param primitiveTypeInfo the field's {@link PrimitiveTypeInfo}
   * @return the {@link Schema.Type} for this field
   */
  private static Schema getType(String name, PrimitiveTypeInfo primitiveTypeInfo) {
    PrimitiveObjectInspector.PrimitiveCategory category = primitiveTypeInfo.getPrimitiveCategory();
    switch (category) {
      case BOOLEAN:
        return Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN));
      case BYTE:
      case SHORT:
      case INT:
        return Schema.nullableOf(Schema.of(Schema.Type.INT));
      case LONG:
        return Schema.nullableOf(Schema.of(Schema.Type.LONG));
      case FLOAT:
        return Schema.nullableOf(Schema.of(Schema.Type.FLOAT));
      case DOUBLE:
        return Schema.nullableOf(Schema.of(Schema.Type.DOUBLE));
      case CHAR:
      case STRING:
      case VARCHAR:
        return Schema.nullableOf(Schema.of(Schema.Type.STRING));
      case BINARY:
        return Schema.nullableOf(Schema.of(Schema.Type.BYTES));
      case TIMESTAMP:
        return Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS));
      case DECIMAL:
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveTypeInfo;
        return Schema.nullableOf(Schema.decimalOf(decimalTypeInfo.getPrecision(), decimalTypeInfo.getScale()));
      // We can support VOID by having Schema type as null but HCatRecord does not support VOID and since we read
      // write through HCatSchema and HCatRecord we are not supporting VOID too for consistent behavior.
      case VOID:
      case DATE:
      case UNKNOWN:
      default:
        throw new IllegalArgumentException(String.format("Table schema contains field '%s' with unsupported type %s",
                                                         name, category.name()));
    }
  }

  /**
   * Generates schema for complex type
   *
   * @param name        field name
   * @param fieldSchema {@link HCatFieldSchema} field schema
   * @return {@link Schema} CDAP compatible schema for the field
   * @throws HCatException throw error in unknown schema type
   */
  private static Schema getComplexType(String name, HCatFieldSchema fieldSchema) throws HCatException {
    switch (fieldSchema.getCategory()) {
      case ARRAY:
        HCatSchema elementSchema = fieldSchema.getArrayElementSchema();
        HCatFieldSchema element = extractSubsSchemaForComplexType(elementSchema);
        Schema arrayTypeSchema;
        if (element.isComplex()) {
          arrayTypeSchema = getComplexType(name, element);
        } else {
          arrayTypeSchema = getType(element.getName(), element.getTypeInfo());
        }
        return Schema.nullableOf(Schema.arrayOf(arrayTypeSchema));
      case MAP:
        PrimitiveTypeInfo mapKeyTypeInfo = fieldSchema.getMapKeyTypeInfo();
        HCatSchema mapValueSchema = fieldSchema.getMapValueSchema();
        HCatFieldSchema hCatFieldSchema = extractSubsSchemaForComplexType(mapValueSchema);
        Schema valueMapSchema = null;
        if (hCatFieldSchema.isComplex()) {
          valueMapSchema = getComplexType(name, hCatFieldSchema);
        } else {
          valueMapSchema = getType(hCatFieldSchema.getName(), hCatFieldSchema.getTypeInfo());
        }
        Schema keySchema = getType(name, mapKeyTypeInfo);
        return Schema.nullableOf(Schema.mapOf(keySchema, valueMapSchema));
      case STRUCT:
        HCatSchema structSubSchema = fieldSchema.getStructSubSchema();
        return Schema.nullableOf(toSchema(name, structSubSchema));
      case PRIMITIVE:
      default:
        throw new RuntimeException(String.format("Unsupported field category %s.", fieldSchema.getCategory()));
    }
  }

  /**
   * Generates HCat compatible {@link HCatFieldSchema} from {@link Schema} for a given field and checks for type
   * matching with existing {@link HCatFieldSchema}.
   *
   * @param fieldName        name of the field in {@link Schema}.
   * @param schema           input {@link Schema}.
   * @param tableFieldSchema existing {@link HCatFieldSchema}.
   * @return {@link HCatFieldSchema} compatible with existing table field.
   * @throws Exception exception if generated field schema is not compatible with existing field schema from table.
   */
  public static HCatFieldSchema getType(String fieldName, Schema schema, HCatFieldSchema tableFieldSchema)
    throws HCatException {
    boolean isSimpleType = schema.isSimpleOrNullableSimple();
    HCatFieldSchema result = null;
    schema = schema.isNullable() ? schema.getNonNullable() : schema;
    Schema.Type type = schema.getType();
    if (!isSimpleType) {
      switch (type) {
        case MAP: {
          checkMatchingSchemaTypes(type, HCatFieldSchema.Category.MAP, tableFieldSchema.getCategory());
          Map.Entry<Schema, Schema> mapSchema = schema.getMapSchema();

          PrimitiveTypeInfo mapKeyTypeInfo = tableFieldSchema.getMapKeyTypeInfo();
          HCatFieldSchema hCatFieldKeySchema = new HCatFieldSchema(null, mapKeyTypeInfo, null);

          HCatFieldSchema hCatFieldValueSchema = extractSubsSchemaForComplexType(tableFieldSchema.getMapValueSchema());

          HCatFieldSchema hCatKeySchema = getType(null, mapSchema.getKey(), hCatFieldKeySchema);
          HCatFieldSchema hCatValueSchema = getType(null, mapSchema.getValue(), hCatFieldValueSchema);

          HCatSchemaUtils.MapBuilder mapSchemaBuilder = HCatSchemaUtils.getMapSchemaBuilder();
          mapSchemaBuilder.withKeyType(hCatKeySchema.getTypeInfo());
          HCatSchema hCatSchema = HCatSchemaUtils.getHCatSchema(hCatValueSchema.getTypeInfo());
          mapSchemaBuilder.withValueSchema(hCatSchema);
          result = HCatFieldSchema.createMapTypeFieldSchema(fieldName, hCatKeySchema.getTypeInfo(), hCatSchema,
                                                            null);
          break;
        }
        case RECORD: {
          checkMatchingSchemaTypes(type, HCatFieldSchema.Category.STRUCT, tableFieldSchema.getCategory());
          HCatSchema structSubSchema = tableFieldSchema.getStructSubSchema();

          HCatSchemaUtils.CollectionBuilder structSchemaBuilder = HCatSchemaUtils.getStructSchemaBuilder();
          for (Schema.Field field : schema.getFields()) {
            HCatFieldSchema hCatFieldSchema = structSubSchema.get(field.getName());
            structSchemaBuilder.addField(getType(field.getName(), field.getSchema(), hCatFieldSchema));
          }
          HCatSchema hCatFieldSchema = structSchemaBuilder.build();
          result = new HCatFieldSchema(fieldName, HCatFieldSchema.Type.STRUCT, hCatFieldSchema, null);
          break;
        }
        case ARRAY: {
          checkMatchingSchemaTypes(type, HCatFieldSchema.Category.ARRAY, tableFieldSchema.getCategory());
          HCatSchema arrayElementSchema = tableFieldSchema.getArrayElementSchema();
          HCatFieldSchema hCatFieldSchema = arrayElementSchema.get(0);
          Schema componentSchema = schema.getComponentSchema();
          HCatFieldSchema arrayItemSchema = getType(null, componentSchema, hCatFieldSchema);
          HCatSchema arraySchema = HCatSchemaUtils.getListSchemaBuilder().addField(arrayItemSchema).build();
          result = new HCatFieldSchema(fieldName, HCatFieldSchema.Type.ARRAY, arraySchema, null);
          break;
        }
        default:
          throw new IllegalArgumentException(String.format(
            "Schema contains field '%s' with unsupported type %s. " +
              "You should provide an schema with this field dropped to work with this table.", fieldName, type));
      }
    } else {
      if (tableFieldSchema.isComplex()) {
        throw new IllegalArgumentException(
          String.format("Table field '%s' of complex type is not compatible with primitive type provided in schema"
            , fieldName));
      }
      Schema.LogicalType logicalType = schema.getLogicalType();
      PrimitiveTypeInfo typeInfo;
      if (logicalType != null) {
        switch (logicalType) {
          case DECIMAL:
            typeInfo = TypeInfoFactory.decimalTypeInfo;
            break;
          case TIMESTAMP_MICROS:
          case TIMESTAMP_MILLIS:
            typeInfo = TypeInfoFactory.timestampTypeInfo;
            break;
          default:
            throw new IllegalArgumentException(String.format(
              "Schema contains field '%s' with unsupported type %s. " +
                "You should provide an schema with this field dropped to work with this table.", fieldName, type));
        }
        return new HCatFieldSchema(fieldName, typeInfo, null);
      }

      switch (type) {
        case BOOLEAN:
          typeInfo = TypeInfoFactory.booleanTypeInfo;
          break;
        case INT: {
          //check if the table field type is of following
          PrimitiveTypeInfo tableSchemaFieldTypeInfo = tableFieldSchema.getTypeInfo();
          if (!(tableSchemaFieldTypeInfo.equals(TypeInfoFactory.byteTypeInfo)
            || tableSchemaFieldTypeInfo.equals(TypeInfoFactory.shortTypeInfo)
            || tableSchemaFieldTypeInfo.equals(TypeInfoFactory.intTypeInfo))) {
            throw new IllegalArgumentException(
              String.format("Field of type '%s' is compatible only with the following types: %s, %s, %s.",
                            type, TypeInfoFactory.byteTypeInfo,
                            TypeInfoFactory.shortTypeInfo, TypeInfoFactory.intTypeInfo)
            );
          }
          typeInfo = tableSchemaFieldTypeInfo;
          break;
        }
        case LONG:
          typeInfo = TypeInfoFactory.longTypeInfo;
          break;
        case FLOAT:
          typeInfo = TypeInfoFactory.floatTypeInfo;
          break;
        case DOUBLE:
          typeInfo = TypeInfoFactory.doubleTypeInfo;
          break;
        case STRING: {
          PrimitiveTypeInfo tableSchemaFieldTypeInfo = tableFieldSchema.getTypeInfo();
          PrimitiveObjectInspector.PrimitiveCategory primitiveCategory =
            tableSchemaFieldTypeInfo.getPrimitiveCategory();
          if (!(primitiveCategory.equals(PrimitiveObjectInspector.PrimitiveCategory.CHAR)
            || primitiveCategory.equals(PrimitiveObjectInspector.PrimitiveCategory.VARCHAR)
            || primitiveCategory.equals(PrimitiveObjectInspector.PrimitiveCategory.STRING))) {
            throw new IllegalArgumentException(
              String.format("Field of type '%s' is compatible only with the following types: %s, %s, %s.",
                            type, TypeInfoFactory.varcharTypeInfo,
                            TypeInfoFactory.charTypeInfo, TypeInfoFactory.stringTypeInfo)
            );
          }

          typeInfo = TypeInfoFactory.getPrimitiveTypeInfo(tableFieldSchema.getTypeString());
          break;
        }
        case BYTES:
          typeInfo = TypeInfoFactory.binaryTypeInfo;
          break;
        default:
          throw new IllegalArgumentException(String.format(
            "Schema contains field '%s' with unsupported type %s. " +
              "You should provide an schema with this field dropped to work with this table.", fieldName, type));
      }


      result = new HCatFieldSchema(fieldName, typeInfo, null);
    }

    if (!result.equals(tableFieldSchema)) {
      throw new IllegalArgumentException(String.format("Field of type '%s' is not compatible with type '%s'.",
                                                       result, tableFieldSchema));
    }

    return result;
  }

  /**
   * Extracts {@link HCatFieldSchema} for complex type like array and map
   *
   * @param hCatSchema {@link HCatFieldSchema} base schema of hcatalog complex type
   * @return {@link HCatFieldSchema} return field schema type
   */
  public static HCatFieldSchema extractSubsSchemaForComplexType(HCatSchema hCatSchema) {
    return hCatSchema.getFields().stream().findFirst().orElseThrow(RuntimeException::new);
  }

  /**
   * Validates schema type compatibility.
   *
   * @param type              {@link Schema.Type} schema type.
   * @param category          {@link HCatFieldSchema.Category} HCat schema category.
   * @param tableFromCategory {@link HCatFieldSchema.Category} HCat schema category to compare to.
   */
  private static void checkMatchingSchemaTypes(Schema.Type type, HCatFieldSchema.Category category,
                                               HCatFieldSchema.Category tableFromCategory) {
    Preconditions.checkState(category.equals(tableFromCategory), "Incompatible schema provided %s for field name %s," +
      " but in table schema is %s", type, category, tableFromCategory);
  }
}
