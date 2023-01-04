package com.gamma.skybase.build.server.etl.utils;

import com.gamma.skybase.build.server.service.SkybaseCacheServiceHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class SDPLookupUpdate {
    private List<Map<String, Object>> sdpRecords = new ArrayList<>();
    private static final int BATCH_SIZE = 5000;

    public void updateSDPMainBalanceLookup(String msisdn, String dayId, double balance) {
        /* prepare record */
        Map<String, Object> record = new HashMap<>();
        record.put("_id", dayId + "|" + msisdn);
        record.put("balance", balance);
        record.put("day_id", dayId);
        record.put("update_time", new Date());
        sdpRecords.add(record);

        if (sdpRecords.size() >= BATCH_SIZE){
            updateSDPMainBalanceLookupEntries();
        }
    }

    public void updateSDPMainBalanceLookupEntries() {
        if (!sdpRecords.isEmpty()){
            SkybaseCacheServiceHandler.instance().updateSDPMainBalanceLookupEntries(sdpRecords);
            sdpRecords.clear();
        }
    }
}
