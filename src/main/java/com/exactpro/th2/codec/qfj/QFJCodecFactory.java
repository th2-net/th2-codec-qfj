package com.exactpro.th2.codec.qfj;

import com.exactpro.th2.codec.api.IPipelineCodec;
import com.exactpro.th2.codec.api.IPipelineCodecContext;
import com.exactpro.th2.codec.api.IPipelineCodecFactory;
import com.exactpro.th2.codec.api.IPipelineCodecSettings;
import com.exactpro.th2.common.schema.dictionary.DictionaryType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import quickfix.ConfigError;
import quickfix.DataDictionary;

import java.io.InputStream;

public class QFJCodecFactory implements IPipelineCodecFactory {

    private DataDictionary dataDictionary = null;
    private DataDictionary transportDataDictionary = null;
    private DataDictionary appDataDictionary = null;
    private QFJCodecSettings settings;

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
    public @NotNull IPipelineCodec create(@Nullable IPipelineCodecSettings settings) {
        this.settings = (QFJCodecSettings) settings;
        return new QFJCodec(settings, dataDictionary, transportDataDictionary, appDataDictionary);
    }

    @Override
    public void init(@NotNull IPipelineCodecContext codecContext) {

        try {
            if (settings.isFixt()) {
                transportDataDictionary = new DataDictionary((codecContext.get(DictionaryType.MAIN)));
                appDataDictionary = new DataDictionary(codecContext.get(DictionaryType.LEVEL1));
            } else {
                dataDictionary = new DataDictionary(codecContext.get(DictionaryType.MAIN));
            }

        } catch (ConfigError error) {
            throw new IllegalStateException("Failed to load DataDictionary", error);
        }
    }

    @Override
    public void init(@NotNull InputStream inputStream) {

    }
}
