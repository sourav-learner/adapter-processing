package com.gamma.skybase.build.server.etl.decoder.mobily_sgsn;

import com.gamma.components.commons.constants.GammaConstants;
import com.gamma.components.structure.IDatum;
import com.gamma.decoder.ascii.FixedLengthDecoder;
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
public class MobilySGSNFileExecutor extends MobilySGSNFileProcessor {
    private static final Logger logger = LoggerFactory.getLogger(MobilySGSNFileExecutor.class);
    private List<LinkedHashMap<String, Object>> jsonRecords;
    String[] headers = new String[]{"SEQUENCENUMBER" , "BATCHNUMBER" , "SERVICETYPE" , "TOTALTYPE" , "CALLINDICATOR" , "MSISDN" , "ACCESSPOINTNAME" , "CALL_EVENT_START_TIMESTAMP" , "TOTAL_CALL_EVENT_DURATION" , "PARTIALCALLINDICATOR" , "CNUMBER" , "IMSI" , "NODEID" , "SERVICEID" , "EQUIPMENTIDA" , "EQUIPMENTIDAB" , "CELLNAME" , "LOCATIONAREAID" , "MSCLASSWORK" , "MSRN" , "MSCID" , "CALL_REFERENCE_NUMBER" , "RECORDINGENTITYIDENTIFICATION" , "PDP_TYPE" , "PDPADDRESS" , "CAUSE_FOR_TERMINATION" , "PDPCONTEXTSTARTTIMESTAMP" , "CHARGING_ID" , "RESERVED" , "DATAVOLUMEINCOMING" , "DATAVOLUMEOUTGOING" , "SGSN_Address" , "GGSN_Address" , "PORTINFLAG" };

    static TreeMap<Integer, Integer> columnIndexes;

    static {
        columnIndexes = new TreeMap<>();
        columnIndexes.put(0,  6);
        columnIndexes.put(6,  18);
        columnIndexes.put(18, 20);
        columnIndexes.put(20, 23);
        columnIndexes.put(23, 26);
        columnIndexes.put(26, 46);
        columnIndexes.put(46, 70);
        columnIndexes.put(70, 84);
        columnIndexes.put(84, 92);
        columnIndexes.put(92, 93);
        columnIndexes.put(93, 117);
        columnIndexes.put(117,132);
        columnIndexes.put(132,134);
        columnIndexes.put(134,138);
        columnIndexes.put(138,154);
        columnIndexes.put(154,170);
        columnIndexes.put(170,185);
        columnIndexes.put(185,190);
        columnIndexes.put(190,191);
        columnIndexes.put(191,209);
        columnIndexes.put(209,214);
        columnIndexes.put(214,224);
        columnIndexes.put(224,228);
        columnIndexes.put(228,232);
        columnIndexes.put(232,247);
        columnIndexes.put(247,249);
        columnIndexes.put(249,263);
        columnIndexes.put(263,273);
        columnIndexes.put(273,279);
        columnIndexes.put(279,294);
        columnIndexes.put(294,309);
        columnIndexes.put(309,324);
        columnIndexes.put(324,339);
        columnIndexes.put(339,340);
    }

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

        FixedLengthDecoder decoder = null;
        try {
            long recCount = 0;

            decoder = new FixedLengthDecoder(fileName, columnIndexes, headers, 0);
            while (decoder.hasNext()) {
                try {
                    LinkedHashMap<String, Object> record = decoder.next();
                    if (jsonOutputRequired) jsonRecords.add(record);

                    record.put("fileName", metadata.decompFileName);
                    processRecord(record, enrichment);
                } catch (Exception e) {
                    metadata.comments = "Parsing issues";
                    metadata.noOfBadRecord++;
                    logger.error(dataSource.getAdapterName() , " -> " , e.getMessage(), e);
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
                    handleEvents("FCT_SGSN", data);
            }
        }
    }

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }
}