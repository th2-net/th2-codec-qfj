package com.exactpro.th2.codec.qfj;

import com.exactpro.th2.codec.api.IPipelineCodecSettings;

public class QFJCodecSettings implements IPipelineCodecSettings {
    private boolean fixt = true;

    public boolean isFixt() {
        return fixt;
    }

    public void setFixt(boolean fixt) {
        this.fixt = fixt;
    }
}
