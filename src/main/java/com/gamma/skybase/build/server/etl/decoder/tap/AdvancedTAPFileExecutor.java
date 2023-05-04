package com.gamma.skybase.build.server.etl.decoder.tap;

import com.gamma.components.structure.IDatum;
import com.gamma.skybase.adapter.tap.TAPFileExecutor;

import java.util.Map;

/**
 * Created by abhi on 2019-04-24
 */
public class AdvancedTAPFileExecutor extends TAPFileExecutor {

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }
}
