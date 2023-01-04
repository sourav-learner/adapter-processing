package com.gamma.skybase.build.server.etl.decoder.ccn;

import com.gamma.components.structure.IDatum;
import com.gamma.skybase.decoders.ccn.ChargingSystemFileExecutor;
import com.gamma.skybase.decoders.msc.MSCFileExecutor;

import java.util.Map;

/**
 * Created by abhi on 2019-03-23
 */
public class AdvancedCCNFileExecutor extends ChargingSystemFileExecutor {

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }
}
