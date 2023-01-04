package com.gamma.skybase.build.server.etl.decoder.nrtrde;

import com.gamma.components.structure.IDatum;
import com.gamma.skybase.adapter.nrtrde.NRTRDEFileExecutor;

import java.util.Map;

/**
 * Created by abhi on 2019-04-24
 */
public class AdvancedNRTRDEFileExecutor extends NRTRDEFileExecutor {

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }
}
