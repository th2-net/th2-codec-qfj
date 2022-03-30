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
package com.exactpro.th2.codec.qfj;

import com.exactpro.th2.codec.api.IPipelineCodec;
import com.exactpro.th2.codec.api.IPipelineCodecContext;
import com.exactpro.th2.codec.api.IPipelineCodecFactory;
import com.exactpro.th2.codec.api.IPipelineCodecSettings;
import com.exactpro.th2.common.schema.dictionary.DictionaryType;
import com.google.auto.service.AutoService;

import org.apache.commons.lang3.ObjectUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import quickfix.ConfigError;
import quickfix.DataDictionary;

import java.io.InputStream;
import java.util.List;
import java.util.Set;

@AutoService(IPipelineCodecFactory.class)
public class QFJCodecFactory implements IPipelineCodecFactory {

    private static final List<String> PROTOCOLS = List.of("FIX");

    private IPipelineCodecContext codecContext;

    @NotNull
    @Override
    public Set<String> getProtocols() { return Set.of(PROTOCOLS.get(0)); }

    @NotNull
    @Override
    public String getProtocol() {
        return PROTOCOLS.get(0);
    }

    @Override
    public @NotNull Class<? extends IPipelineCodecSettings> getSettingsClass() {
        return QFJCodecSettings.class;
    }

    @Override
    public void close() {
    }

    @Override
    public @NotNull IPipelineCodec create(@Nullable IPipelineCodecSettings settings) {
        QFJCodecSettings qfjCodecSettings = ObjectUtils.defaultIfNull((QFJCodecSettings) settings, new QFJCodecSettings());

        try {
            if (qfjCodecSettings.isFixt()) {
                DataDictionary transport = new DataDictionary(codecContext.get(DictionaryType.MAIN));
                DataDictionary appDataDictionary = new DataDictionary(codecContext.get(DictionaryType.LEVEL1));
                return new QFJCodec(qfjCodecSettings, null, transport, appDataDictionary);
            } else {
                DataDictionary dataDictionary = new DataDictionary(codecContext.get(DictionaryType.MAIN));
                return new QFJCodec(qfjCodecSettings, dataDictionary, null, null);
            }
        } catch (ConfigError error) {
            throw new IllegalStateException("Failed to load DataDictionary", error);
        }
    }

    @Override
    public void init(@NotNull IPipelineCodecContext codecContext) {
        this.codecContext = codecContext;
    }

    @Override
    public void init(@NotNull InputStream inputStream) { }
}
