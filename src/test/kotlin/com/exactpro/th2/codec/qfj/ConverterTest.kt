/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.codec.qfj

import com.exactpro.th2.codec.util.Converter.convertToType
import com.exactpro.th2.codec.util.Converter.decodeFromType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import quickfix.FieldType


class ConverterTest {

    @Test
    fun `decode from type test`() {
        assertEquals("2022-03-22T09:55:32.537", decodeFromType(FieldType.UTCTIMESTAMP, "20220322-09:55:32.537"))
        assertEquals("2022-03-22", decodeFromType(FieldType.UTCDATEONLY, "20220322"))
        assertEquals("09:55:32.537", decodeFromType(FieldType.UTCTIMEONLY, "09:55:32.537"))
    }

    @Test
    fun encodeToTypeTest() {
        assertEquals("20220322-09:55:32.537", convertToType(FieldType.UTCTIMESTAMP, "2022-03-22T09:55:32.537"))
        assertEquals("20220322", convertToType(FieldType.UTCDATEONLY, "2022-03-22"))
        assertEquals("09:55:32.537", convertToType(FieldType.UTCTIMEONLY, "09:55:32.537"))
    }
}