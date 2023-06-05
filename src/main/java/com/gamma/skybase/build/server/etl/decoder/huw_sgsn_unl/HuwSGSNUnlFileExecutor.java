package com.gamma.skybase.build.server.etl.decoder.huw_sgsn_unl;

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
import java.util.*;

@SuppressWarnings("Duplicates")
public class HuwSGSNUnlFileExecutor extends HuwSGSNUnlFileProcessor {
    private static final Logger logger = LoggerFactory.getLogger(HuwSGSNUnlFileExecutor.class);
    private List<LinkedHashMap<String, Object>> jsonRecords;

    String[] headers = new String[]{"SEQUENCENUMBER" , "BATCHNUMBER" , "SERVICETYPE" , "TOTALTYPE" , "CALLINDICATOR" , "MSISDN" , "ACCESSPOINTNAME" , "CALL_EVENT_START_TIMESTAMP" , "TOTAL_CALL_EVENT_DURATION" , "PARTIALCALLINDICATOR" , "CNUMBER" , "IMSI" , "NODEID" , "SERVICEID" , "EQUIPMENTIDA" , "EQUIPMENTIDAB" , "CELLNAME" , "LOCATIONAREAID" , "MSCLASSWORK" , "MSRN" , "MSCID" , "CALL_REFERENCE_NUMBER" , "RECORDINGENTITYIDENTIFICATION" , "PDP_TYPE" , "PDPADDRESS" , "CAUSE_FOR_TERMINATION" , "PDPCONTEXTSTARTTIMESTAMP" , "CHARGING_ID" , "RESERVED" , "DATAVOLUMEINCOMING" , "DATAVOLUMEOUTGOING" , "SGSN_Address" , "GGSN_Address" , "PORTINFLAG" , "UNKNOWN"};

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

            decoder = new DelimitedFileDecoder(fileName, ',', headers, 0);
            while (decoder.hasNext()) {
                try {
                    LinkedHashMap<String, Object> record = decoder.next();
                    if (jsonOutputRequired) jsonRecords.add(record);
                    record.put("_SEQUENCE_NUMBER",recCount);
                    record.put("fileName", metadata.decompFileName);
                    processRecord(record, enrichment);
                } catch (Exception e) {
                    e.printStackTrace();
                    metadata.comments = "Parsing issues";
                    metadata.noOfBadRecord++;
                    logger.error(dataSource.getAdapterName() + " -> " , e.getMessage(), e);
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
                    handleEvents("FCT_HUW_SGSN_UNL", data);
            }
        }
    }

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }
}