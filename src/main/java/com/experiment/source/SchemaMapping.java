/*
 * Copyright 2025 Yashwanth Gowda N S
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.experiment.source;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialect.ColumnConverter;
import io.confluent.connect.jdbc.source.ColumnMapping;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

public final class SchemaMapping {

  public static SchemaMapping create(
      String schemaName,
      ResultSetMetaData metadata,
      DatabaseDialect dialect
  ) throws SQLException {
    Map<ColumnId, ColumnDefinition> colDefns = dialect.describeColumns(metadata);
    Map<String, ColumnConverter> colConvertersByFieldName = new LinkedHashMap<>();
    SchemaBuilder builder = SchemaBuilder.struct().name(schemaName);
    int columnNumber = 0;
    for (ColumnDefinition colDefn : colDefns.values()) {
      ++columnNumber;
      String fieldName = dialect.addFieldToSchema(colDefn, builder);
      if (fieldName == null) {
        continue;
      }
      Field field = builder.field(fieldName);
      ColumnMapping mapping = new ColumnMapping(colDefn, columnNumber, field);
      ColumnConverter converter = dialect.createColumnConverter(mapping);
      colConvertersByFieldName.put(fieldName, converter);
    }
    Schema schema = builder.build();
    return new SchemaMapping(schema, colConvertersByFieldName);
  }

  private final Schema schema;
  private final List<FieldSetter> fieldSetters;

  private SchemaMapping(
      Schema schema,
      Map<String, ColumnConverter> convertersByFieldName
  ) {
    assert schema != null;
    assert convertersByFieldName != null;
    assert !convertersByFieldName.isEmpty();
    this.schema = schema;
    List<FieldSetter> fieldSetters = new ArrayList<>(convertersByFieldName.size());
    for (Map.Entry<String, ColumnConverter> entry : convertersByFieldName.entrySet()) {
      ColumnConverter converter = entry.getValue();
      Field field = schema.field(entry.getKey());
      assert field != null;
      fieldSetters.add(new FieldSetter(converter, field));
    }
    this.fieldSetters = Collections.unmodifiableList(fieldSetters);
  }

  public Schema schema() {
    return schema;
  }

  List<FieldSetter> fieldSetters() {
    return fieldSetters;
  }

  @Override
  public String toString() {
    return "Mapping for " + schema.name();
  }

  public static final class FieldSetter {

    private final ColumnConverter converter;
    private final Field field;

    private FieldSetter(
        ColumnConverter converter,
        Field field
    ) {
      this.converter = converter;
      this.field = field;
    }

    public Field field() {
      return field;
    }

    void setField(
        Struct struct,
        ResultSet resultSet
    ) throws SQLException, IOException {
      Object value = this.converter.convert(resultSet);
      if (resultSet.wasNull()) {
        struct.put(field, null);
      } else {
        struct.put(field, value);
      }
    }

    @Override
    public String toString() {
      return field.name();
    }
  }
}
