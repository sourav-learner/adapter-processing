package com.gamma.skybase.build.server.etl.decoder.psms;

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
import java.util.*;

/**
 * Created by abhi on 02/02/22
 */
public class JinnyPSMSFileExecutor extends JinnyPSMSFileProcessor{
    private static final Logger logger = LoggerFactory.getLogger(JinnyPSMSFileExecutor.class);
    private List<LinkedHashMap<String, Object>> jsonRecords;

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

        String rawFileName = metadata.decompFileName;
        String  [] headers = null;
        if (rawFileName.toLowerCase().startsWith("tlr")) {
            headers = new String[]{"from_number", "to_number", "record_timestamp", "message_type", "status", "error_code", "asp_name", "application_name", "sc_id ", "sc_connection", "message_id", "charge_msisdn", "charge_amount", "charge_method", "charge_seq_id", "charge_min_credit", "charge_service_name", "charge_reason", "timestamp1", "timestamp2"};
        } else if (rawFileName.toLowerCase().startsWith("jmrcdr")) {
            headers = new String[]{"from_number", "to_number", "record_timestamp", "message_type", "status", "error_code", "asp_name", "application_name", "sc_id ", "sc_connection", "message_id", "charge_msisdn", "charge_amount", "charge_method", "charge_seq_id", "charge_min_credit", "charge_service_name", "charge_reason", "timestamp1", "timestamp2", "timestamp3", "si_protocol", "ston", "snpi", "dton", "dnpi", "datacoding"};
        }
        DelimitedFileDecoder decoder = null;
        try {
            long recCount = 0;
            decoder = new DelimitedFileDecoder(fileName, '|', headers, 0);
            while (decoder.hasNext()) {
                try {
                    LinkedHashMap<String, Object> record = decoder.next();
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
                if (data != null) handleEvents("JINNY_PSMS_FCT", data);
            }
        }
    }

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }
}
