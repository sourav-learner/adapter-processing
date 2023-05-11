package com.gamma.skybase.build.server.etl.decoder.med_tapin;

import com.gamma.components.commons.constants.GammaConstants;
import com.gamma.components.structure.IDatum;
import com.gamma.decoder.ascii.DelimitedFileDecoder;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@SuppressWarnings("Duplicates")
public class MedTAPINFileExecutor extends MedTAPINFileProcessor {
    private static final Logger logger = LoggerFactory.getLogger(MedTAPINFileExecutor.class);
    private List<LinkedHashMap<String, Object>> jsonRecords;
    String[] headers = new String[]{"TAP_REC_ID" , "CALL_TERMINATING_FLAG" , "A_PARTY_NUMBER" , "B_PARTY_NUMBER" , "S_ID" , "IMSI" , "CALL_TIMESTAMP" , "DURATION" , "TADIG_CODE" , "TAPFILE_SEQ_NO" , "UTC_OFFSET_TIMEZONE" , "SECONDS_PER_BILLING_CYCLE" , "AIRTIME_CHARGE" , "PSTN_CHARGE" , "TOTAL_CHARGE" , "CALL_TYPE" , "SLAB_ID" , "TARIFFPLAN_ID" , "TADIG_GROUP_ID" , "NETWORK_ID" , "ERROR_CODE" , "ERROR_DESCRIPTION" , "PARTNER_CODE" , "VOISE_SUBSCRIBER_YN" , "RECORD_PROCESS_STATUS" , "TAP_RATED_DATE" , "VOLUME_OUTGOING" , "TOTAL_VOLUME" , "TYPE_OF_SERVICE" , "TELESERVICE_CODE" , "TAP_AIRTIME_CHARGE" , "TAP_TOLL_CHARGE" , "TAP_TOTAL_CHARGE" , "IMEI_NO" , "MSC_ID" , "VOLUME_INCOMING" , "FLEXI_COL1"};

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

        IEnrichment enrichment = null;
        try {
            Class<?> exec = Class.forName(dataSource.getTxExecClass());
            enrichment = (IEnrichment) exec.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            logger.error(e.getMessage(), e);
        }

        DelimitedFileDecoder decoder = null;
        try {
            long recCount = 1;

            decoder = new DelimitedFileDecoder(fileName, ',', headers, 1);
            while (decoder.hasNext()) {
                try {
                    LinkedHashMap<String, Object> record = decoder.next();
                    if (jsonOutputRequired) jsonRecords.add(record);
                    record.put("_SEQUENCE_NUMBER",recCount);
                    record.put("fileName", metadata.decompFileName);
                    processRecord(record, enrichment);
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

            if (jsonOutputRequired) {
                FileWriter writer = new FileWriter("out" + GammaConstants.PATH_SEP + fn + ".json");
                writer.write(gson.toJson(jsonRecords));
                writer.flush();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (decoder != null) decoder.close();
        }
    }

    private void processRecord(LinkedHashMap<String, Object> record, IEnrichment enrichment) throws Exception {
        if (enrichment != null) {
            MEnrichmentReq request = new MEnrichmentReq();
            request.setRequest(record);
            MEnrichmentResponse response = enrichment.transform(request);
            if (response.isResponseCode()) {
                LinkedHashMap<String, Object> data = response.getResponse();
                metadata.noOfParsedRecord++;
                if (data != null)
                    handleEvents("FCT_MED_TAPIN_VOICE_SMS_GPRS", data);
            }
        }
    }

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }
}