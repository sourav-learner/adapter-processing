package com.gamma.skybase.build.server.etl.decoder.hlr;

import com.gamma.components.structure.IDatum;
import com.gamma.decoder.eric.hlr.adapter.HLRFileExecutor;

import java.util.Map;

public class AdvancedHLRFileExecutor extends HLRFileExecutor {

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }
}
