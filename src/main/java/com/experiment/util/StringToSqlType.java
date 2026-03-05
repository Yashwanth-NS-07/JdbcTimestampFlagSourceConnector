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
package com.experiment.util;

import org.apache.kafka.connect.data.Schema;

import java.math.BigDecimal;

public class StringToSqlType {
    public static Object convert(String rawValue, Schema schema) {
        final String value = rawValue.trim();
        // if logical data type
        if(schema.name() != null && schema.name().equals("org.apache.kafka.connect.data.Decimal")) {
            return new BigDecimal(value);
        }
        // if primitive data type
        try {
            switch (schema.type()) {
                case INT8:
                    return Byte.parseByte(value);
                case INT16:
                    return Short.parseShort(value);
                case INT32:
                    return Integer.parseInt(value);
                case INT64:
                    return Long.parseLong(value);
                case FLOAT32:
                    return Float.parseFloat(value);
                case FLOAT64:
                    return Double.parseDouble(value);
                case BOOLEAN:
                    return Boolean.parseBoolean(value);
                case STRING:
                    return value;
                default:
                    throw new IllegalArgumentException("Unsupported data type for Flag Column, schema type: " + schema.type());
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid number format for " + schema.type() + ": " + value, e);
        } catch (Exception e) {
            throw new IllegalArgumentException("Conversion error for " + schema.type() + ": " + value, e);
        }
    }
}
