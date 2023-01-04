package com.gamma.skybase.build.server.service;

import com.gamma.components.storage.factory.AppDbHandler;
import com.gamma.components.storage.factory.IDBHandler;
import com.gamma.skybase.build.server.etl.utils.DMSSaleRecord;

import java.util.List;
import java.util.Map;

@AppDbHandler(name = "skybase_build")
public interface ISkybaseCacheServiceDBHandler extends IDBHandler {

    boolean isDuplicate(String cdrId);

    void addCdrInValidationPool(List<Map<String, Object>> data);

    void addEntriesInCRMLookup(String crmLookupTableName, List<Map<String, Object>> records);

    double getSDPMainBalanceFromLookup(String msisdn, String dayId);

    void updateSDPMainBalanceLookupEntries(List<Map<String, Object>> sdpRecords);

    double getSDPMainBalanceFromLookup(String key);

    Map<String, Double> loadAllSDPBalanceLookups(String dayId);

    void writeDMSCacheableEntries(List<Map<String, Object>> records);

    List<DMSSaleRecord> loadDMSRecordsFromDB();

    DMSSaleRecord getDMSSalesLookup(String serial);
}
