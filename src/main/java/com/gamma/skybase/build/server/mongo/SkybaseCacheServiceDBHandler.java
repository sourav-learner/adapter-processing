package com.gamma.skybase.build.server.mongo;

import com.gamma.components.commons.DateUtility;
import com.gamma.components.storage.mongo.MongoDbConnection;
import com.gamma.components.storage.mongo.MongoUtils;
import com.gamma.skybase.build.server.etl.utils.DMSSaleRecord;
import com.gamma.skybase.build.server.service.ISkybaseCacheServiceDBHandler;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.time.ZoneId;
import java.util.*;

public class SkybaseCacheServiceDBHandler implements ISkybaseCacheServiceDBHandler {

    private MongoDbConnection mdbc = MongoDbConnection.instance();

    @Override
    public boolean isDuplicate(String cdrId) {
        MongoDatabase database = mdbc.getDatabase();
        Document where = new Document("_id", cdrId);
        try (MongoCursor<Document> cursor = database.getCollection(CollectionNames.SKYBASE_VOUCHER_CDR_CONTAINER.toString())
                .find(where)
                .iterator()) {
            return cursor.hasNext();
        }
    }

    @Override
    public void addCdrInValidationPool(List<Map<String, Object>> data) {
        if (data == null || data.isEmpty()) return;
        MongoCollection<Document> collection = mdbc.getDatabase().getCollection(CollectionNames.SKYBASE_VOUCHER_CDR_CONTAINER.toString());
        List<Document> documents = new ArrayList<>();
        for (Map<String, Object> d: data){
            Document document = new Document();
            document.putAll(d);
            documents.add(document);
        }
        if (!documents.isEmpty()){
            MongoUtils.insertManyWithRetry(collection, documents, 3, 10);
        }
    }

    @Override
    public void addEntriesInCRMLookup(String crmLookupTableName, List<Map<String, Object>> records) {
        if (records == null || records.isEmpty()) return;
        MongoCollection<Document> collection = mdbc.getDatabase().getCollection(crmLookupTableName);
        List<Map<String, Document>> dataset = new ArrayList<>();
        for (Map<String, Object> d: records){
            Document row = new Document();
            row.putAll(d);
            row.put("_id", d.get("served_msisdn") + "|" + d.get("served_imsi"));

            Map<String, Document> item = new HashMap<>();
            Document where = new Document("_id", row.get("_id"));
            item.put("where", where);
            item.put("set", row);
            dataset.add(item);
        }
        if (!dataset.isEmpty()){
            MongoUtils.upsertManyWithRetry(collection, dataset, 3, 30);
        }
    }

    @Override
    public double getSDPMainBalanceFromLookup(String msisdn, String dayId) {
        MongoDatabase database = mdbc.getDatabase();
        Document where = new Document("_id", dayId + "|" + msisdn);
        try (MongoCursor<Document> cursor = database.getCollection(CollectionNames.SDP_MAIN_BALANCE_LOOKUP.toString())
                .find(where)
                .iterator()) {
            if (cursor.hasNext()){
                return (Double) cursor.next().getOrDefault("balance", 0d);
            }
        }
        return 0d;
    }

    @Override
    public void updateSDPMainBalanceLookupEntries(List<Map<String, Object>> records) {
        if (records == null || records.isEmpty()) return;
        MongoCollection<Document> collection = mdbc.getDatabase().getCollection(CollectionNames.SDP_MAIN_BALANCE_LOOKUP.toString());
//        List<Document> dataset = new ArrayList<>();
//        for (Map<String, Object> d: records){
//            Document row = new Document();
//            row.putAll(d);
//            dataset.add(row);
//        }
//        if (!dataset.isEmpty()){
//            MongoUtils.insertManyWithRetry(collection, dataset, 3, 30);
//        }

        List<Map<String, Document>> request = new ArrayList<>();
        for (Map<String, Object> item: records){
            Map<String, Document> record = new HashMap<>();
            Document where = new Document();
            Document set = new Document(item);
            where.put("_id", item.get("_id"));
            set.remove("_id");

            record.put("where", where);
            record.put("set", set);
            request.add(record);
        }
        if (!request.isEmpty()) {
            MongoUtils.upsertManyWithRetry(collection, request, 3, 100);
        }
    }

    @Override
    public double getSDPMainBalanceFromLookup(String key) {
        MongoDatabase database = mdbc.getDatabase();
        Document where = new Document("_id", key);
        try (MongoCursor<Document> cursor = database.getCollection(CollectionNames.SDP_MAIN_BALANCE_LOOKUP.toString())
                .find(where)
                .iterator()) {
            if (cursor.hasNext()){
                return (Double) cursor.next().getOrDefault("balance", 0d);
            }
        }
        return 0d;
    }

    @Override
    public Map<String, Double> loadAllSDPBalanceLookups(String dayId) {
        MongoDatabase database = mdbc.getDatabase();
        Map<String, Double> lookups = new HashMap<>();
        Document where = new Document("day_id", dayId);
        try (MongoCursor<Document> cursor = database.getCollection(CollectionNames.SDP_MAIN_BALANCE_LOOKUP.toString())
                .find(where)
                .iterator()) {
            while (cursor.hasNext()){
                Document d = cursor.next();
                lookups.put(d.getString("_id"), (Double) d.getOrDefault("balance", 0d));
            }
        }
        return lookups;
    }

    @Override
    public void writeDMSCacheableEntries(List<Map<String, Object>> records) {
        if (records == null || records.isEmpty()) return;
        MongoCollection<Document> collection = mdbc.getDatabase().getCollection("cache_dms_voucher_sales");

        List<Map<String, Document>> request = new ArrayList<>();
        for (Map<String, Object> item: records){
            Map<String, Document> record = new HashMap<>();
            Document where = new Document();
            Document set = new Document(item);
            where.put("_id", item.get("_id"));
            set.remove("_id");

            record.put("where", where);
            record.put("set", set);
            request.add(record);
        }
        if (!request.isEmpty()) {
            MongoUtils.upsertManyWithRetry(collection, request, 3, 100);
        }
    }

    @Override
    public List<DMSSaleRecord> loadDMSRecordsFromDB() {
        MongoDatabase database = mdbc.getDatabase();
        List<DMSSaleRecord> lookups = new ArrayList<>();

        try (MongoCursor<Document> cursor = database.getCollection("cache_dms_voucher_sales")
                .find()
                .sort(new Document("submission_date", 1))
                .iterator()) {
            while (cursor.hasNext()){
                Document d = cursor.next();
                DMSSaleRecord record = new DMSSaleRecord();
                record.setRecordId(d.getString("_id"));
                record.setStartSerial(d.getString("start_serial"));
                record.setEndSerial(d.getString("end_serial"));
                record.setName(d.getString("name"));
                record.setEquipName(d.getString("equip_name"));
                record.setSubmissionDate(d.getDate("submission_date"));
                record.setYearId(DateUtility.getYearId(record.getSubmissionDate()));
                record.setMonthName(record.getSubmissionDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate().getMonth().name());
                lookups.add(record);
            }
        }
        return lookups;
    }

    @Override
    public DMSSaleRecord getDMSSalesLookup(String serial) {
        DMSSaleRecord record = null;
        MongoDatabase database = mdbc.getDatabase();
        Document where = new Document("start_serial", new Document("$lte", serial)).append("end_serial", new Document("$gte", serial));
        try (MongoCursor<Document> cursor = database.getCollection("cache_dms_voucher_sales")
                .find(where)
                .sort(new Document("start_serial", 1).append("end_serial", 1).append("submission_date", -1))
                .limit(1)
                .iterator()) {
            if (cursor.hasNext()){
                Document d = cursor.next();
                record = new DMSSaleRecord();
                record.setRecordId(d.getString("_id"));
                record.setStartSerial(d.getString("start_serial"));
                record.setEndSerial(d.getString("end_serial"));
                record.setName(d.getString("name"));
                record.setEquipName(d.getString("equip_name"));
                record.setSubmissionDate(d.getDate("submission_date"));
                record.setYearId(DateUtility.getYearId(record.getSubmissionDate()));
                record.setMonthName(record.getSubmissionDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate().getMonth().name());
            }
        }
        return record;
    }
}
