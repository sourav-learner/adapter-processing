package com.gamma.skybase.build.server.etl.decoder.mmsc;

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
public class MMSCFileExecutor extends MMSCFileProcessor {

    private static final Logger logger = LoggerFactory.getLogger(MMSCFileExecutor.class);
    private Gson gson;
    private List<LinkedHashMap<String, Object>> jsonRecords;

    @Override
    public void parseFile(String fileName) throws Exception {
        boolean jsonOutputRequired = dataSource.isRawJsonEnabled();
        String fn = null;
        if (jsonOutputRequired) {
            fn = new File(fileName).getName();
            gson = new GsonBuilder().setPrettyPrinting().create();
            jsonRecords = new LinkedList<>();
        }

        IEnrichment enrichment = null;
        try {
            Class exec = Class.forName(dataSource.getTxExecClass());
            enrichment = (IEnrichment) exec.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            logger.error(e.getMessage(), e);
        }

        String[] headers = {"MESSAGE_ID", "CALLER_ID", "CALLER_ID_IMSI", "SENDER", "SENDER_IMSI", "RECIPIENT",
                "RECIPIENT_IMSI", "MESSAGE_SIZE", "SUBMISSION_TIME_STAMP", "EARLIEST_DELIVERY_TIMESTAMP", "EXPIRATION_TIME_STAMP", "UNKNOWN_1",
                "MESSAGE_TYPE", "BEARER_TYPE", "CONTENT_TYPE", "MESSAGE_CLASS", "SENDER_HIDE_REQUESTED", "DELIVERY_REPORT_REQUESTED",
                "READ_REPLY_REQUESTED", "STORAGE_DURATION", "PARTY_TO_BILL", "MM7_SERVICE_CODE", "MM_STATUS", "FORWARDING_INDICATION",
                "CONVERSION_OF_MEDIA_TYPES", "PRIORITY", "VASPID", "VASID", "GPRS_USERNAME", "GPRS_CHARGING_ID",
                "PREPAID_OR_NOT", "GGSN", "SGSN", "CHARGING_INDICATOR"};;

        try (DelimitedFileDecoder decoder = new DelimitedFileDecoder(fileName, '|', headers, 0)) {
            long recCount = 0;
            while (decoder.hasNext()) {
                LinkedHashMap<String, Object> record = decoder.next();
                record.put("fileName", this.metadata.decompFileURL.substring(this.metadata.decompFileURL.lastIndexOf(File.separator) + 1));
                try {
                    if (jsonOutputRequired) {
                        jsonRecords.add(record);
                    }
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
            FileWriter writer = new FileWriter("out" + GammaConstants.PATH_SEP + fn + ".json");
            writer.write(gson.toJson(jsonRecords));
            writer.flush();
        }
    }

    private void processRecord(LinkedHashMap<String, Object> record, IEnrichment enrichment) throws Exception {
        if (enrichment != null) {
            MEnrichmentReq request = new MEnrichmentReq();
            request.setRequest(record);
            MEnrichmentResponse response = enrichment.transform(request);
            LinkedHashMap<String, Object> data = response.getResponse();
            handleEvents("MMSC", data);
        }
    }

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }
}