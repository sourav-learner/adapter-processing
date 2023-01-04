package com.gamma.skybase.build.server.etl.decoder.lookup.crm;

import com.gamma.components.exceptions.AppUnexpectedException;
import com.gamma.components.structure.IDatum;
import com.gamma.decoder.ascii.DelimitedFileDecoder;
import com.gamma.skybase.build.server.service.SkybaseCacheServiceHandler;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.skybase.contract.decoders.SkybaseDeserializerException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.*;

import static com.gamma.components.commons.constants.GammaConstants.PATH_SEP;

public class CRMLookupFileExecutor extends CRMLookupFileProcessor {
    private static final Logger logger = LoggerFactory.getLogger(CRMLookupFileExecutor.class);
    private List<LinkedHashMap<String, Object>> jsonRecords;
    private List<Map<String, Object>> crmLookupRecords;
    private String crmLookupTableName;

    @Override
    public void parseFile(String fileName) throws Exception {
        boolean jsonOutputRequired = dataSource.isRawJsonEnabled();
        String fn = null;
        Gson gson = null;
        if (jsonOutputRequired) {
            fn = new File(fileName).getName();
            gson = new GsonBuilder().setPrettyPrinting().create();
            jsonRecords = new LinkedList<>();
        }
        crmLookupRecords = new ArrayList<>();

        IEnrichment enrichment = null;
        try {
            Class<?> exec = Class.forName(dataSource.getTxExecClass());
            enrichment = (IEnrichment) exec.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            logger.error(e.getMessage(), e);
        }

        try (DelimitedFileDecoder decoder = new DelimitedFileDecoder(fileName, ',', true, true)) {
            logger.info("Adapter {} decoding File {} ", dataSource.getAdapterName(), new File(fileName).getName());
            long recCount = 0;
            while (decoder.hasNext()) {
                try {
                    LinkedHashMap<String, Object> record = decoder.next();
                    if (jsonOutputRequired) {
                        jsonRecords.add(record);
                    }
                    record.put("fileName", metadata.decompFileName);
                    processRecord(record, enrichment);
                    metadata.noOfParsedRecord++;
                } catch (Exception e) {
                    metadata.comments = "Parsing issues";
                    metadata.noOfBadRecord++;
                    logger.error(dataSource.getAdapterName() + " -> " + e.getMessage(), e);
                }
                recCount++;
                metadata.totalNoOfRecords = recCount;
            }
            if (metadata.noOfBadRecord > 0) metadata.status = "partial";
            if (metadata.noOfParsedRecord == 0 && recCount > 0) metadata.status = "failed";
        }
        if (jsonOutputRequired) {
            FileWriter writer = new FileWriter("out" + PATH_SEP + dataSource.getAdapterName() + PATH_SEP + fn + ".json");
            writer.write(gson.toJson(jsonRecords));
            writer.flush();
        }
    }

    private void processRecord(LinkedHashMap<String, Object> record, IEnrichment enrichment) throws Exception {
        if (enrichment != null) {
            MEnrichmentReq request = new MEnrichmentReq();
            request.setRequest(record);
            MEnrichmentResponse response = enrichment.transform(request);
            if (response.isResponseCode()) {
                LinkedHashMap<String, Object> data = response.getResponse();
                String eventName = "LOOKUP_STREAM_FCT";
                try {
                    eventName = dataSource.getDataDef().getEvents().values().stream().findFirst().get().getEventName();
                    if (crmLookupTableName == null) crmLookupTableName = dataSource.getDataDef().getEventByName(eventName).getTableName().toLowerCase();
                }catch (Exception ignored){}

                Map<String, Object> lookupRecord = new HashMap<>(data);
                crmLookupRecords.add(lookupRecord);
                if (crmLookupRecords.size() >= 5000){
                    SkybaseCacheServiceHandler.instance().addEntriesInCRMLookup(crmLookupTableName, crmLookupRecords);
                    crmLookupRecords.clear();
                }

                handleEvents(eventName, data);
            }else {
                throw new AppUnexpectedException("Record enrichment failed.");
            }
        }
    }

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }

    @Override
    public void flush() throws SkybaseDeserializerException {
        super.flush();

        //flush crm lookup entries
        if (crmLookupRecords != null && !crmLookupRecords.isEmpty()){
            SkybaseCacheServiceHandler.instance().addEntriesInCRMLookup(crmLookupTableName, crmLookupRecords);
            crmLookupRecords.clear();
        }
    }
}
