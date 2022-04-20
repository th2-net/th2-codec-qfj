/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.codec.util;

import org.jetbrains.annotations.NotNull;
import quickfix.FieldConvertError;
import quickfix.FieldType;
import quickfix.UtcTimestampPrecision;
import quickfix.field.converter.BooleanConverter;
import quickfix.field.converter.DecimalConverter;
import quickfix.field.converter.IntConverter;
import quickfix.field.converter.UtcDateOnlyConverter;
import quickfix.field.converter.UtcTimeOnlyConverter;
import quickfix.field.converter.UtcTimestampConverter;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;

public class Converter {
    public static String decodeFromType(FieldType fieldType, String value) {
        try {
            switch (fieldType) {
                case UTCTIMESTAMP:
                    return UtcTimestampConverter.convertToLocalDateTime(value).toString();
                case UTCTIMEONLY:
                    return UtcTimeOnlyConverter.convertToLocalTime(value).toString();
                case UTCDATEONLY:
                    return UtcDateOnlyConverter.convertToLocalDate(value).toString();
                case FLOAT:
                case AMT:
                case PRICE:
                case PRICEOFFSET:
                case QTY:
                case PERCENTAGE:
                    return DecimalConverter.convert(value).toPlainString();
                case INT:
                case LENGTH:
                case NUMINGROUP:
                case SEQNUM:
                    return String.valueOf(IntConverter.convert(value));
                case BOOLEAN:
                    return String.valueOf(BooleanConverter.convert(value));
                default:
                    return value;
            }
        } catch (FieldConvertError ex) {
            throw new IllegalArgumentException("cannot convert field " + value + " of type " + fieldType, ex);
        }
    }

    public static String convertToType(FieldType fieldType, String value) {
        switch (fieldType) {
            case UTCTIMESTAMP:
                return toTimestamp(fieldType, value);
            case UTCTIMEONLY:
                return toTimeOnly(fieldType, value);
            case UTCDATEONLY:
                return toDateOnly(fieldType, value);
            case FLOAT:
            case AMT:
            case PRICE:
            case PRICEOFFSET:
            case QTY:
            case PERCENTAGE:
                return toDecimal(fieldType, value);
            case INT:
            case LENGTH:
            case NUMINGROUP:
            case SEQNUM:
                return toInt(fieldType, value);
            case BOOLEAN:
                return toBool(fieldType, value);
            default:
                return value;
        }
    }

    private static String toBool(FieldType fieldType, String value) {
        boolean bool;
        try {
            if (!"true".equals(value) && !"false".equals(value)) {
                bool = BooleanConverter.convert(value);
            } else {
                bool = "true".equals(value);
            }
        } catch (FieldConvertError error) {
            throw new IllegalArgumentException("incorrect value " + value + " for type: " + fieldType, error);
        }
        return BooleanConverter.convert(bool);
    }

    private static String toInt(FieldType fieldType, String value) {
        int intValue;
        try {
            intValue = IntConverter.convert(value);
        } catch (FieldConvertError error) {
            throw new IllegalArgumentException("incorrect value " + value + " for type: " + fieldType, error);
        }
        return IntConverter.convert(intValue);
    }

    private static String toDecimal(FieldType fieldType, String value) {
        BigDecimal decimal;
        try {
            decimal = DecimalConverter.convert(value);
        } catch (FieldConvertError error) {
            throw new IllegalArgumentException("incorrect value " + value + " for type: " + fieldType, error);
        }
        return DecimalConverter.convert(decimal);
    }

    @NotNull
    private static String toTimestamp(FieldType fieldType, String value) {
        LocalDateTime localDateTime;
        try {
            localDateTime = LocalDateTime.parse(value);
        } catch (DateTimeParseException ex) {
            try {
                localDateTime = UtcTimestampConverter.convertToLocalDateTime(value);
            } catch (FieldConvertError error) {
                throw new IllegalArgumentException("incorrect value " + value + " for type: " + fieldType, error);
            }
        }
        return UtcTimestampConverter.convert(localDateTime, calculatePrecision(localDateTime.getNano()));
    }

    @NotNull
    private static String toTimeOnly(FieldType fieldType, String value) {
        LocalTime localTime;
        try {
            localTime = LocalTime.parse(value);
        } catch (DateTimeParseException ex) {
            try {
                localTime = UtcTimeOnlyConverter.convertToLocalTime(value);
            } catch (FieldConvertError error) {
                throw new IllegalArgumentException("incorrect value " + value + " for type: " + fieldType, error);
            }
        }
        return UtcTimeOnlyConverter.convert(localTime, calculatePrecision(localTime.getNano()));
    }

    @NotNull
    private static String toDateOnly(FieldType fieldType, String value) {
        LocalDate localDate;
        try {
            localDate = LocalDate.parse(value);
        } catch (DateTimeParseException ex) {
            try {
                localDate = UtcDateOnlyConverter.convertToLocalDate(value);
            } catch (FieldConvertError error) {
                throw new IllegalArgumentException("incorrect value " + value + " for type: " + fieldType, error);
            }
        }
        return UtcDateOnlyConverter.convert(localDate);
    }

    private static UtcTimestampPrecision calculatePrecision(int nanos) {
        if (nanos == 0) {
            return UtcTimestampPrecision.SECONDS;
        }
        if (nanos % 1_000 != 0) {
            return UtcTimestampPrecision.NANOS;
        }
        if (nanos % 1_000_000 != 0) {
            return UtcTimestampPrecision.MICROS;
        }
        if (nanos % 1_000_000_000 != 0) {
            return UtcTimestampPrecision.MILLIS;
        }
        throw new IllegalArgumentException("nanos have incorrect value: " + nanos);
    }
}
