package com.gamma.skybase.build.server.etl.decoder.air;

import com.gamma.components.structure.IDatum;
import com.gamma.skybase.decoders.air.AIRFileExecutor;

import java.util.Map;

/**
 * Created by abhi on 2019-04-24
 */
public class AdvancedAIRFileExecutor extends AIRFileExecutor {

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }
}
