package com.exactpro.th2.codec.qfj;

import com.exactpro.th2.codec.util.Converter;
import org.junit.jupiter.api.Test;
import quickfix.FieldType;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConverterTest {

    @Test
    public void decodeFromTypeTest() {
        assertEquals("2022-03-22T09:55:32.537", Converter.decodeFromType(FieldType.UTCTIMESTAMP, "20220322-09:55:32.537"));
        assertEquals("2022-03-22", Converter.decodeFromType(FieldType.UTCDATEONLY, "20220322"));
        assertEquals("09:55:32.537", Converter.decodeFromType(FieldType.UTCTIMEONLY, "09:55:32.537"));
    }

    @Test
    public void encodeToTypeTest() {
        assertEquals("20220322-09:55:32.537", Converter.convertToType(FieldType.UTCTIMESTAMP, "2022-03-22T09:55:32.537"));
        assertEquals("20220322", Converter.convertToType(FieldType.UTCDATEONLY, "2022-03-22"));
        assertEquals("09:55:32.537", Converter.convertToType(FieldType.UTCTIMEONLY, "09:55:32.537"));
    }

}
