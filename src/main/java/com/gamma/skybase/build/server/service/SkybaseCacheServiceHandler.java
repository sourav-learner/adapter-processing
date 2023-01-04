package com.gamma.skybase.build.server.service;

import com.gamma.components.commons.DateUtility;
import com.gamma.components.commons.app.AppConfig;
import com.gamma.components.exceptions.AppUnexpectedException;
import com.gamma.components.storage.factory.AppsDbServiceFactory;
import com.gamma.skybase.build.server.etl.utils.DMSSaleRecord;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;
import net.openhft.hashing.LongHashFunction;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

@Slf4j
public class SkybaseCacheServiceHandler {
    private static final int CDR_WRITE_BATCH_SIZE = 5000;
    private static SkybaseCacheServiceHandler serviceHandler;

    /* duplicate CDRs validation */
    private ISkybaseCacheServiceDBHandler dbHandler;
    private static CacheLoader<String, Boolean> loader;
    private static LoadingCache<String, Boolean> cache;
    private Map<String, List<Map<String, Object>>> cdrEntries = new ConcurrentHashMap<>();
    public static final String AMOUNT_FORMAT = "#.000000";

    /* SDP Main lookup */
    private static CacheLoader<String, Double> sdpLoader;
    private static LoadingCache<String, Double> sdpLookup;
    private static int sdpLookupCacheExpiryInterval = 5;
    private static Date sdpLookupCacheLastLoadedTime = null;
    private List<Map<String, Object>> sdpRecords = new ArrayList<>();

    /* DMS Sales lookup */
    private static CacheLoader<Long, DMSSaleRecord>  dmsLoader;
    private static LoadingCache<Long, DMSSaleRecord> dmsLookup;
    private static final int dmsLookupCacheExpiryInterval = 6;
    private static Date dmsLookupCacheLastLoadedTime = null;

    public static synchronized SkybaseCacheServiceHandler instance(){
        if(serviceHandler == null){
            serviceHandler = new SkybaseCacheServiceHandler();
        }
        return serviceHandler;
    }

    private SkybaseCacheServiceHandler(){
        String dbName = AppConfig.instance().getModuleDbProperty("app.module.default.db");
        this.dbHandler = (ISkybaseCacheServiceDBHandler) AppsDbServiceFactory.instance(AppConfig.instance().getProperty("app.deployment.id"))
                .getDbHandler("skybase_build", dbName);

        /* CDR Dup Check */
        loader = new CacheLoader<String, Boolean>() {
            @Override
            public Boolean load(String key) {
                return dbHandler.isDuplicate(key);
            }
        };
        cache = CacheBuilder.newBuilder().maximumSize(100_000)
                .expireAfterAccess(3, TimeUnit.HOURS)
                .build(loader);

        /* SDP Main */
        sdpLoader = new CacheLoader<String, Double>() {
            @Override
            public Double load(String key) {
                return getSDPBalanceLookup(key);
            }
        };
        sdpLookup = CacheBuilder.newBuilder().maximumSize(25_000_000)
                .expireAfterAccess(sdpLookupCacheExpiryInterval, TimeUnit.HOURS)
                .build(sdpLoader);

        /* DMS Sales */
        dmsLoader = new CacheLoader<Long, DMSSaleRecord>() {
            @Override
            public DMSSaleRecord load(Long key) {
                return getDMSSalesLookup(key);
            }
        };
        dmsLookup = CacheBuilder.newBuilder().maximumSize(300_000_000)
                .expireAfterAccess(dmsLookupCacheExpiryInterval, TimeUnit.HOURS)
                .build(dmsLoader);
    }

    public String generateWicashCdrKey(Map<String, Object> record, String adapter){

        String a = String.valueOf(record.getOrDefault("caxact",""));
        String b = String.valueOf(record.getOrDefault("customer_id",""));
        String c = String.valueOf(record.getOrDefault("cachknum",""));
        String d = String.valueOf(record.getOrDefault("cachkdate",""));
        String e = String.valueOf(record.getOrDefault("caglcash",""));
        String f = String.valueOf(record.getOrDefault("catype",""));
        String g = String.valueOf(record.getOrDefault("carem",""));
        String h = String.valueOf(record.getOrDefault("cachkamt_gl",0));

        /* fix added to capture duplicate cdrs that prefixed by extra decimal 000 */
        h = new DecimalFormat(AMOUNT_FORMAT).format(Double.parseDouble(h));

        String key = adapter + a + b + c + d + e + f + g + h;
        return zeroAllocationHash(key);
    }

    public String generateRatedTapoutCdrKey(Map<String, Object> record, String adapter){

        StringBuilder key = new StringBuilder().append(adapter);
        List<String> numeric = Arrays.asList("duration_volume", "rated_volume", "rounded_volume", "zero_rounded_volume_volume",
                "eff_chrg_inf_xcurr_disc_amount", "eff_chrg_inf_hcurr_disc_amount", "eff_charge_info_xcurr_tax");
        for (Map.Entry<String, Object> entry: record.entrySet()){
            if ("fileName".equalsIgnoreCase(entry.getKey())) continue;
            if (numeric.contains(entry.getKey())){
                key.append(new DecimalFormat(AMOUNT_FORMAT).format(Double.parseDouble(String.valueOf(entry.getValue()))));
            }else {
                key.append(entry.getValue());
            }
        }
        return zeroAllocationHash(key.toString());
    }

    public void addRatedTapoutRecordsInCdrContainer(String source, Collection<Map<String, Object>> records){

        String SOURCE_FORMAT = "yyyy-MM-dd HH:mm:ss";
        ThreadLocal<DateTimeFormatter> SOURCE_FORMAT_SDF = ThreadLocal.withInitial(() -> DateTimeFormat.forPattern(SOURCE_FORMAT));
        List<Map<String, Object>> data = new ArrayList<>();
        for (Map<String, Object> record: records) {
            Map<String, Object> row = new HashMap<>();

            String cdrId = generateRatedTapoutCdrKey(record, source);
            String cd = String.valueOf(record.getOrDefault("event_date", ""));

            row.put("_id", cdrId);
            row.put("source", source);
            row.put("file_name",record.get("fileName"));
            row.put("xdr_date", SOURCE_FORMAT_SDF.get().parseDateTime(cd).toDate());
            data.add(row);
        }
        writeRatedTapoutCDREntries(source, data, false);
    }

    public synchronized void writeRatedTapoutCDREntries(String source, List<Map<String, Object>> data, boolean forceFlush){
        writeCDREntries(source, data, forceFlush);
    }

    public static String zeroAllocationHash(String string) {
        return String.valueOf(LongHashFunction.xx().hashChars(string));
    }

    public void addWicashRecordsInCdrContainer(String source, Collection<Map<String, Object>> records){

        String SOURCE_FORMAT = "yyyy-MM-dd HH:mm:ss";
        ThreadLocal<DateTimeFormatter> SOURCE_FORMAT_SDF = ThreadLocal.withInitial(() -> DateTimeFormat.forPattern(SOURCE_FORMAT));
        List<Map<String, Object>> data = new ArrayList<>();
        for (Map<String, Object> record: records) {
            Map<String, Object> row = new HashMap<>();

            String cdrId = generateWicashCdrKey(record, source);
            String cd = String.valueOf(record.getOrDefault("cachkdate", ""));

            row.put("_id", cdrId);
            row.put("source", source);
            row.put("file_name",record.get("fileName"));
            row.put("xdr_date", SOURCE_FORMAT_SDF.get().parseDateTime(cd).toDate());
            data.add(row);
        }
        writeWicashCDREntries(source, data, false);
    }

    public synchronized void writeWicashCDREntries(String source, List<Map<String, Object>> data, boolean forceFlush){
        writeCDREntries(source, data, forceFlush);
    }

    /* Rated-TAPIN */
    public String generateRatedTapinCdrKey(Map<String, Object> record, String adapter){

        StringBuilder key = new StringBuilder().append(adapter);
        List<String> numeric = Arrays.asList("duration_volume", "rated_volume", "rounded_volume", "rated_flat_amount",
                "xfile_charge_amount", "xfile_charge_tax", "uplink_volume_volume", "downlink_volume_volume", "data_volume");
        for (Map.Entry<String, Object> entry: record.entrySet()){
            if ("fileName".equalsIgnoreCase(entry.getKey())) continue;
            if (numeric.contains(entry.getKey())){
                key.append(new DecimalFormat(AMOUNT_FORMAT).format(Double.parseDouble(String.valueOf(entry.getValue()))));
            }else {
                key.append(entry.getValue());
            }
        }
        return zeroAllocationHash(key.toString());
    }

    public void addRatedTapinRecordsInCdrContainer(String source, Collection<Map<String, Object>> records){

        String SOURCE_FORMAT = "yyyy-MM-dd HH:mm:ss";
        ThreadLocal<DateTimeFormatter> SOURCE_FORMAT_SDF = ThreadLocal.withInitial(() -> DateTimeFormat.forPattern(SOURCE_FORMAT));
        List<Map<String, Object>> data = new ArrayList<>();
        for (Map<String, Object> record: records) {
            Map<String, Object> row = new HashMap<>();

            String cdrId = generateRatedTapinCdrKey(record, source);
            String cd = String.valueOf(record.getOrDefault("event_date", ""));

            row.put("_id", cdrId);
            row.put("source", source);
            row.put("file_name",record.get("fileName"));
            row.put("xdr_date", SOURCE_FORMAT_SDF.get().parseDateTime(cd).toDate());
            data.add(row);
        }
        writeRatedTapinCDREntries(source, data, false);
    }

    public synchronized void writeRatedTapinCDREntries(String source, List<Map<String, Object>> data, boolean forceFlush){
        writeCDREntries(source, data, forceFlush);
    }

    /* Interconnect */
    public String generateInterconnectCdrKey(Map<String, Object> record, String adapter){

        StringBuilder key = new StringBuilder().append(adapter);
        List<String> numeric = Arrays.asList("calls_count","rated_flat_amount","duration_volume");
        for (Map.Entry<String, Object> entry: record.entrySet()){
            if ("fileName".equalsIgnoreCase(entry.getKey())) continue;
            if (numeric.contains(entry.getKey())){
                key.append(new DecimalFormat(AMOUNT_FORMAT).format(Double.parseDouble(String.valueOf(entry.getValue()))));
            }else {
                key.append(entry.getValue());
            }
        }
        return zeroAllocationHash(key.toString());
    }

    public String generateInterconnectMarginCdrKey(Map<String, Object> record, String adapter){

        StringBuilder key = new StringBuilder().append(adapter);
        List<String> numeric = Arrays.asList("rated_flat_amount","duration_volume");
        for (Map.Entry<String, Object> entry: record.entrySet()){
            if ("fileName".equalsIgnoreCase(entry.getKey())) continue;
            if (numeric.contains(entry.getKey())){
                key.append(new DecimalFormat(AMOUNT_FORMAT).format(Double.parseDouble(String.valueOf(entry.getValue()))));
            }else {
                key.append(entry.getValue());
            }
        }
        return zeroAllocationHash(key.toString());
    }

    /* MNP */
    public String generateMNPCdrKey(Map<String, Object> record, String adapter){

        List<String> keys = new ArrayList<>();
        List<String> keyColumns = Arrays.asList("msisdn","order_id","modified_time", "status");
        keys.add(adapter);
        for (String col: keyColumns){
            keys.add((String) record.get(col));
        }
        return zeroAllocationHash(StringUtils.join(keys, "|"));
    }

    public String generateEVDCdrKey(Map<String, Object> record, String adapter){

        StringBuilder key = new StringBuilder().append(adapter);
        List<String> numeric = Arrays.asList("amount");
        for (Map.Entry<String, Object> entry: record.entrySet()){
            if ("fileName".equalsIgnoreCase(entry.getKey())) continue;
            if (numeric.contains(entry.getKey())){
                key.append(new DecimalFormat(AMOUNT_FORMAT).format(Double.parseDouble(String.valueOf(entry.getValue()))));
            }else {
                key.append(entry.getValue());
            }
        }
        return zeroAllocationHash(key.toString());
    }

    public String generateEVDMonthlyCdrKey(Map<String, Object> record, String adapter){

        StringBuilder key = new StringBuilder().append(adapter);
        List<String> numeric = Arrays.asList("balance_after");
        for (Map.Entry<String, Object> entry: record.entrySet()){
            if ("fileName".equalsIgnoreCase(entry.getKey())) continue;
            if (numeric.contains(entry.getKey())){
                key.append(new DecimalFormat(AMOUNT_FORMAT).format(Double.parseDouble(String.valueOf(entry.getValue()))));
            }else {
                key.append(entry.getValue());
            }
        }
        return zeroAllocationHash(key.toString());
    }

    public String generateEVDSaleCdrKey(Map<String, Object> record, String adapter){

        StringBuilder key = new StringBuilder().append(adapter);
        List<String> numeric = Arrays.asList("amount");
        for (Map.Entry<String, Object> entry: record.entrySet()){
            if ("fileName".equalsIgnoreCase(entry.getKey())) continue;
            if (numeric.contains(entry.getKey())){
                key.append(new DecimalFormat(AMOUNT_FORMAT).format(Double.parseDouble(String.valueOf(entry.getValue()))));
            }else {
                key.append(entry.getValue());
            }
        }
        return zeroAllocationHash(key.toString());
    }

    public String generateDMSCdrKey(Map<String, Object> record, String adapter){

        StringBuilder key = new StringBuilder().append(adapter);
        List<String> numeric = Arrays.asList("transaction_amt");
        for (Map.Entry<String, Object> entry: record.entrySet()){
            if ("fileName".equalsIgnoreCase(entry.getKey())) continue;
            if (numeric.contains(entry.getKey())){
                key.append(new DecimalFormat(AMOUNT_FORMAT).format(Double.parseDouble(String.valueOf(entry.getValue()))));
            }else {
                key.append(entry.getValue());
            }
        }
        return zeroAllocationHash(key.toString());
    }

    public void addInterconnectRecordsInCdrContainer(String source, Collection<Map<String, Object>> records){

        String SOURCE_FORMAT = "yyyy-MM-dd";
        ThreadLocal<DateTimeFormatter> SOURCE_FORMAT_SDF = ThreadLocal.withInitial(() -> DateTimeFormat.forPattern(SOURCE_FORMAT));
        List<Map<String, Object>> data = new ArrayList<>();
        for (Map<String, Object> record: records) {
            Map<String, Object> row = new HashMap<>();

            String cdrId = generateInterconnectCdrKey(record, source);
            String cd = String.valueOf(record.getOrDefault("event_date", ""));

            row.put("_id", cdrId);
            row.put("source", source);
            row.put("file_name",record.get("fileName"));
            row.put("xdr_date", SOURCE_FORMAT_SDF.get().parseDateTime(cd).toDate());
            data.add(row);
        }
        writeInterconnectCDREntries(source, data, false);
    }

    public void addEVDRecordsInCdrContainer(String source, Collection<Map<String, Object>> records){

        String SOURCE_FORMAT = "yyyy-MM-dd HH:mm:ss";
        ThreadLocal<DateTimeFormatter> SOURCE_FORMAT_SDF = ThreadLocal.withInitial(() -> DateTimeFormat.forPattern(SOURCE_FORMAT));
        List<Map<String, Object>> data = new ArrayList<>();
        for (Map<String, Object> record: records) {
            Map<String, Object> row = new HashMap<>();

            String cdrId = generateEVDCdrKey(record, source);
            String cd = String.valueOf(record.getOrDefault("trans_time", ""));

            row.put("_id", cdrId);
            row.put("source", source);
            row.put("file_name",record.get("fileName"));
            row.put("xdr_date", SOURCE_FORMAT_SDF.get().parseDateTime(cd).toDate());
            data.add(row);
        }
        writeEVDCDREntries(source, data, false);
    }

    public void addEVDMonthlyRecordsInCdrContainer(String source, Collection<Map<String, Object>> records){

        String SOURCE_FORMAT = "yyyy-MM-dd HH:mm:ss";
        ThreadLocal<DateTimeFormatter> SOURCE_FORMAT_SDF = ThreadLocal.withInitial(() -> DateTimeFormat.forPattern(SOURCE_FORMAT));
        List<Map<String, Object>> data = new ArrayList<>();
        for (Map<String, Object> record: records) {
            Map<String, Object> row = new HashMap<>();

            String cdrId = generateEVDMonthlyCdrKey(record, source);
            String cd = String.valueOf(record.getOrDefault("trans_time", ""));

            row.put("_id", cdrId);
            row.put("source", source);
            row.put("file_name",record.get("fileName"));
            row.put("xdr_date", SOURCE_FORMAT_SDF.get().parseDateTime(cd).toDate());
            data.add(row);
        }
        writeEVDMonthlyCDREntries(source, data, false);
    }

    public void addEVDSaleRecordsInCdrContainer(String source, Collection<Map<String, Object>> records){

        String SOURCE_FORMAT = "yyyy-MM-dd HH:mm:ss";
        ThreadLocal<DateTimeFormatter> SOURCE_FORMAT_SDF = ThreadLocal.withInitial(() -> DateTimeFormat.forPattern(SOURCE_FORMAT));
        List<Map<String, Object>> data = new ArrayList<>();
        for (Map<String, Object> record: records) {
            Map<String, Object> row = new HashMap<>();

            String cdrId = generateEVDSaleCdrKey(record, source);
            String cd = String.valueOf(record.getOrDefault("paymentdate", ""));

            row.put("_id", cdrId);
            row.put("source", source);
            row.put("file_name",record.get("fileName"));
            row.put("xdr_date", SOURCE_FORMAT_SDF.get().parseDateTime(cd).toDate());
            data.add(row);
        }
        writeEVDSaleCDREntries(source, data, false);
    }

    public void addDMSRecordsInCdrContainer(String source, Collection<Map<String, Object>> records){

        String SOURCE_FORMAT = "yyyy-MM-dd HH:mm:ss";
        ThreadLocal<DateTimeFormatter> SOURCE_FORMAT_SDF = ThreadLocal.withInitial(() -> DateTimeFormat.forPattern(SOURCE_FORMAT));
        List<Map<String, Object>> data = new ArrayList<>();
        for (Map<String, Object> record: records) {
            Map<String, Object> row = new HashMap<>();

            String cdrId = generateDMSCdrKey(record, source);
            String cd = String.valueOf(record.getOrDefault("submit_date", ""));

            row.put("_id", cdrId);
            row.put("source", source);
            row.put("file_name",record.get("fileName"));
            row.put("xdr_date", SOURCE_FORMAT_SDF.get().parseDateTime(cd).toDate());
            data.add(row);
        }
        writeDMSCDREntries(source, data, false);
    }

    public void addInterconnectMarginRecordsInCdrContainer(String source, Collection<Map<String, Object>> records){

        String SOURCE_FORMAT = "yyyy-MM-dd HH:mm:ss";
        ThreadLocal<DateTimeFormatter> SOURCE_FORMAT_SDF = ThreadLocal.withInitial(() -> DateTimeFormat.forPattern(SOURCE_FORMAT));
        List<Map<String, Object>> data = new ArrayList<>();
        for (Map<String, Object> record: records) {
            Map<String, Object> row = new HashMap<>();

            String cdrId = generateInterconnectMarginCdrKey(record, source);
            String cd = String.valueOf(record.getOrDefault("event_time", ""));

            row.put("_id", cdrId);
            row.put("source", source);
            row.put("file_name",record.get("fileName"));
            row.put("xdr_date", SOURCE_FORMAT_SDF.get().parseDateTime(cd).toDate());
            data.add(row);
        }
        writeInterconnectMarginCDREntries(source, data, false);
    }

    public void addMnpRecordsInCdrContainer(String source, Collection<Map<String, Object>> records){

        String SOURCE_FORMAT = "yyyy-MM-dd HH:mm:ss";
        ThreadLocal<DateTimeFormatter> SOURCE_FORMAT_SDF = ThreadLocal.withInitial(() -> DateTimeFormat.forPattern(SOURCE_FORMAT));
        List<Map<String, Object>> data = new ArrayList<>();
        for (Map<String, Object> record: records) {
            Map<String, Object> row = new HashMap<>();

            String cdrId = generateMNPCdrKey(record, source);
            String cd = String.valueOf(record.getOrDefault("modified_time", ""));

            row.put("_id", cdrId);
            row.put("source", source);
            row.put("file_name",record.get("fileName"));
            row.put("xdr_date", SOURCE_FORMAT_SDF.get().parseDateTime(cd).toDate());
            data.add(row);
        }
        writeMnpCDREntries(source, data, false);
    }

    public synchronized void writeInterconnectCDREntries(String source, List<Map<String, Object>> data, boolean forceFlush){
        writeCDREntries(source, data, forceFlush);
    }

    public synchronized void writeEVDCDREntries(String source, List<Map<String, Object>> data, boolean forceFlush){
        writeCDREntries(source, data, forceFlush);
    }

    public synchronized void writeEVDMonthlyCDREntries(String source, List<Map<String, Object>> data, boolean forceFlush){
        writeCDREntries(source, data, forceFlush);
    }

    public synchronized void writeEVDSaleCDREntries(String source, List<Map<String, Object>> data, boolean forceFlush){
        writeCDREntries(source, data, forceFlush);
    }

    public synchronized void writeDMSCDREntries(String source, List<Map<String, Object>> data, boolean forceFlush){
        writeCDREntries(source, data, forceFlush);
    }

    public synchronized void writeInterconnectMarginCDREntries(String source, List<Map<String, Object>> data, boolean forceFlush){
        writeCDREntries(source, data, forceFlush);
    }

    public synchronized void writeMnpCDREntries(String source, List<Map<String, Object>> data, boolean forceFlush){
        writeCDREntries(source, data, forceFlush);
    }

    /* don't call this method from decoder flush */
    private void writeCDREntries(String source, List<Map<String, Object>> data, boolean forceFlush) {
        if (CollectionUtils.isEmpty(data)) {
            if (forceFlush) {
                dbHandler.addCdrInValidationPool(cdrEntries.get(source));
                cdrEntries.getOrDefault(source, new ArrayList<>()).clear();
            }
            return;
        }
        for (String cdrId : getCDRIds(data)) {
            cache.put(cdrId, true);
        }
        cdrEntries.computeIfAbsent(source, k-> new ArrayList<>());
        cdrEntries.get(source).addAll(data);
        if (cdrEntries.get(source).size() > CDR_WRITE_BATCH_SIZE) {
            dbHandler.addCdrInValidationPool(cdrEntries.get(source));
            cdrEntries.getOrDefault(source, new ArrayList<>()).clear();
        }
    }

    private List<String> getCDRIds(List<Map<String, Object>> data) {
        return data.stream().map(item -> item.get("_id").toString()).collect(Collectors.toList());
    }

    /* common checks for all sources */
    public boolean checkIfDuplicateCDR(String cdrId) {
        try {
            return cache.get(cdrId);
        } catch (ExecutionException e) {
            throw new AppUnexpectedException("Failed checking cache for cdrId : " + cdrId + ", e -> " + e.getMessage(), e);
        }
    }

    public void addEntriesInCRMLookup(String crmLookupTableName, List<Map<String, Object>> records) {
        dbHandler.addEntriesInCRMLookup(crmLookupTableName, records);
    }

    public void updateSDPMainBalanceLookup(String msisdn, String dayId, double balance) {
        /* prepare record */
        Map<String, Object> record = new HashMap<>();
        record.put("_id", dayId + "|" + msisdn);
        record.put("balance", balance);
        record.put("day_id", dayId);
        record.put("update_time", new Date());
        sdpRecords.add(record);

        if (sdpRecords.size() >= 5000){
            updateSDPMainBalanceLookupEntries();
        }
    }

    public void updateSDPMainBalanceLookupEntries() {
        if (!sdpRecords.isEmpty()){
            dbHandler.updateSDPMainBalanceLookupEntries(sdpRecords);
            sdpRecords.clear();
        }
    }

    public void updateSDPMainBalanceLookupEntries(List<Map<String, Object>> records) {
        if (!records.isEmpty()){
            dbHandler.updateSDPMainBalanceLookupEntries(records);
        }
    }

    private Double getSDPBalanceLookup(String key) {
        if (sdpLookupCacheLastLoadedTime == null || DateUtility.subtractHours(new Date(), sdpLookupCacheExpiryInterval).after(sdpLookupCacheLastLoadedTime)){
            initializeSDPBalanceLookupCache(key.split("\\|")[0]);
        }
        return getSDPMainBalanceFromLookup(key);
    }

    private synchronized void initializeSDPBalanceLookupCache(String dayId) {
        if (sdpLookupCacheLastLoadedTime == null || DateUtility.subtractHours(new Date(), sdpLookupCacheExpiryInterval).after(sdpLookupCacheLastLoadedTime)){
            log.info("Updating SDP lookups cache from DB for day - {}", dayId);
            Map<String, Double> lookups = dbHandler.loadAllSDPBalanceLookups(dayId);
            if (lookups != null && !lookups.isEmpty()){
                sdpLookup.putAll(lookups);
                log.info("Updating SDP lookups cache from DB for day - {}, Total entries - {} finished.", dayId, lookups.size());
            }
            log.info("Nothing to load into SDP cache.");
            sdpLookupCacheLastLoadedTime = new Date();
        }
    }

    public double getSDPMainBalanceFromLookup(String msisdn, String dayId) {
        String key = dayId + "|" + msisdn;
        try {
            return sdpLookup.get(key);
        } catch (ExecutionException e) {
            log.error(e.getMessage(), e);
        }
        return 0D;
    }

    private double getSDPMainBalanceFromLookup(String key) {
        return dbHandler.getSDPMainBalanceFromLookup(key);
    }

    public void writeDMSCacheableEntries(List<Map<String, Object>> records) {
        dbHandler.writeDMSCacheableEntries(records);
    }

    public DMSSaleRecord getDMSSalesLookup(long key) {
        if (dmsLookupCacheLastLoadedTime == null || DateUtility.subtractHours(new Date(), dmsLookupCacheExpiryInterval).after(dmsLookupCacheLastLoadedTime)){
            initializeDMSLookupCache();
        }
        return getDMSSaleEntryFromLookup(key);
    }

    private synchronized void initializeDMSLookupCache() {
        if (dmsLookupCacheLastLoadedTime == null || DateUtility.subtractHours(new Date(), dmsLookupCacheExpiryInterval).after(dmsLookupCacheLastLoadedTime)){
            log.info("Updating DMS lookups cache from DB");
            List<DMSSaleRecord> entries = dbHandler.loadDMSRecordsFromDB();
            if (entries != null && !entries.isEmpty()){
                for (DMSSaleRecord dmsr: entries){
                    List<Long> range = LongStream.rangeClosed(Long.parseLong(dmsr.getStartSerial()), Long.parseLong(dmsr.getEndSerial()))
                            .boxed().collect(Collectors.toList());
                    range.forEach(r-> dmsLookup.put(r, dmsr));
                }
                log.info("Updated DMS lookups cache from DB. Total entries - {}", dmsLookup.size());
            }else {
                log.info("Nothing to load into DMS cache.");
            }
            dmsLookupCacheLastLoadedTime = new Date();
        }
    }

    private DMSSaleRecord getDMSSaleEntryFromLookup(long serialNumber) {
        try{
            return dmsLookup.get(serialNumber);
        }catch (Exception ignored){}
        return null;
    }
}
