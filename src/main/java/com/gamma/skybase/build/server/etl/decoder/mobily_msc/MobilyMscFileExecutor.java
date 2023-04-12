package com.gamma.skybase.build.server.etl.decoder.mobily_msc;

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
public class MobilyMscFileExecutor extends MobilyMscFileProcessor {
    private static final Logger logger = LoggerFactory.getLogger(MobilyMscFileExecutor.class);
    private List<LinkedHashMap<String, Object>> jsonRecords;
    String[] headers = new String[]{"SEQUENCE_NUMBER" , "BATCH_NUMBER" , "SERVICE_CLASS" , "TOTAL_TYPE" , "CALL_INDICATOR" , "A_NUMBER" , "B_NUMBER" , "START_DATE" , "START_TIME" , "DURATION" , "A_PARTY_CATEGORY" , "B_PARTY_CATEGORY" , "INCOMING_TRUNK" , "OUTGOING_TRUNK" , "PARTIAL_CALL_INDICATOR" , "C_NUMBER" , "IMSI" , "SERVICE_TYPE" , "SERVICE_ID" , "EQUIP_ID_A" , "EQUIP_ID_B" , "CELL_NAME_FIRST" , "CELL_NAME_LAST" , "LOCATION_AREA_ID_LAST" , "LOCATION_AREA_ID_FIRST" , "MS_CLASS_MARK" , "DATA_VOLUME" , "DATA_VOLUME_REF" , "MSRN" , "MSC_ID" , "RECORDING_ENTITY_TYPE" , "CAMEL_INDICATOR" , "CAMEL_SERVER_ADDRESS" , "CAMEL_SERVICE_LEVEL" , "CAMEL_SERVICE_KEY" , "CAMEL_MSC_ADDRESS" , "CAMEL_REFERENCE_NUMBER" , "CAMEL_DESTINATION_NUMBER" , "CAMEL_INITIATED_CF_INDICATOR" , "PORTINFLAG"};

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
        columnIndexes.put(70, 78);
        columnIndexes.put(78, 84);
        columnIndexes.put(84, 92);
        columnIndexes.put(92, 94);
        columnIndexes.put(94,96);
        columnIndexes.put(96,104);
        columnIndexes.put(104,112);
        columnIndexes.put(112,113);
        columnIndexes.put(113,137);
        columnIndexes.put(137,152);
        columnIndexes.put(152,154);
        columnIndexes.put(154,158);
        columnIndexes.put(158,174);
        columnIndexes.put(174,190);
        columnIndexes.put(190,195);
        columnIndexes.put(195,200);
        columnIndexes.put(200,205);
        columnIndexes.put(205,210);
        columnIndexes.put(210,211);
        columnIndexes.put(211,217);
        columnIndexes.put(217,223);
        columnIndexes.put(223,241);
        columnIndexes.put(241,256);
        columnIndexes.put(256,257);
        columnIndexes.put(257,258);
        columnIndexes.put(258,282);
        columnIndexes.put(282,284);
        columnIndexes.put(284,294);
        columnIndexes.put(294,309);
        columnIndexes.put(309,325);
        columnIndexes.put(325,349);
        columnIndexes.put(349,350);
        columnIndexes.put(350,351);
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
                    handleEvents("FCT_Mob_MSC", data);
            }
        }
    }

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }
}