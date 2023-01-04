package com.gamma.skybase.build.server.etl.decoder.ggsn;

import com.gamma.components.structure.IDatum;
import com.gamma.skybase.decoders.ggsn.GGSNFileExecutor;

import java.util.Map;

/**
 * Created by abhi on 13/05/20
 */
public class AdvancedGGSNFileExecutor extends GGSNFileExecutor {

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }
}
