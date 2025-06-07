package com.experiment.util;

import java.math.BigDecimal;
import java.sql.Types;
import java.time.format.DateTimeParseException;

public class StringToSqlType {
    public static Object convert(String value, int sqlType) {
        try {
            switch (sqlType) {
                // INTEGER, SMALLINT, TINYINT
                case Types.INTEGER:
                case Types.SMALLINT:
                case Types.TINYINT:
                    return Integer.parseInt(value.trim());

                // BIGINT
                case Types.BIGINT:
                    return Long.parseLong(value.trim());

                // DECIMAL, NUMERIC
                case Types.DECIMAL:
                case Types.NUMERIC:
                    return new BigDecimal(value.trim());

                // FLOAT, REAL
                case Types.FLOAT:
                case Types.REAL:
                    return Float.parseFloat(value.trim());

                // DOUBLE
                case Types.DOUBLE:
                    return Double.parseDouble(value.trim());

                // BOOLEAN, BIT
                case Types.BOOLEAN:
                case Types.BIT:
                    return Boolean.parseBoolean(value.trim());

                // CHAR, VARCHAR, LONGVARCHAR, NCHAR, NVARCHAR, LONGNVARCHAR
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    return value; // Already a strin

                default:
                    throw new IllegalArgumentException("Unsupported SQL data type for Flag Column: " + sqlType);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid number format for " + sqlType + ": " + value, e);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid date/time format for " + sqlType + ": " + value, e);
        } catch (Exception e) {
            throw new IllegalArgumentException("Conversion error for " + sqlType + ": " + value, e);
        }
    }
}
