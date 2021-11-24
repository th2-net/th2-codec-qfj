package com.exactpro.th2.codec.qfj;

import com.exactpro.th2.codec.api.IPipelineCodec;
import com.exactpro.th2.codec.api.IPipelineCodecContext;
import com.exactpro.th2.codec.api.IPipelineCodecFactory;
import com.exactpro.th2.codec.api.IPipelineCodecSettings;
import com.exactpro.th2.common.schema.dictionary.DictionaryType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.ConfigError;
import quickfix.DataDictionary;

import java.io.InputStream;
import java.util.Objects;

public class QFJCodecFactory implements IPipelineCodecFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(QFJCodecFactory.class);

    private DataDictionary dataDictionary = null;
    private DataDictionary transportDataDictionary = null;
    private DataDictionary appDataDictionary = null;

    @Override
    public @NotNull String getProtocol() {
        return "FIX";
    }

    @Override
    public @NotNull Class<? extends IPipelineCodecSettings> getSettingsClass() {
        return QFJCodecSettings.class;
    }

    @Override
    public void close() {
    }

    @Override
    public  @NotNull IPipelineCodec create(@Nullable IPipelineCodecSettings settings) {
        return new QFJCodec(settings, dataDictionary, transportDataDictionary, appDataDictionary);
    }

    @Override
    public void init(@NotNull IPipelineCodecContext codecContext) {

        try {
//            dataDictionary = new DataDictionary(codecContext.get(DictionaryType.MAIN));
            transportDataDictionary = new DataDictionary((codecContext.get(DictionaryType.LEVEL1)));
            appDataDictionary = new DataDictionary(codecContext.get(DictionaryType.LEVEL2));

        } catch (ConfigError error) {
            LOGGER.error("Failed to load DataDictionary");
        }
    }

    @Override
    public void init(@NotNull InputStream inputStream) {

    }
}
