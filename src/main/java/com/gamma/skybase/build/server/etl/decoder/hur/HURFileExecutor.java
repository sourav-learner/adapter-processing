package com.gamma.skybase.build.server.etl.decoder.hur;

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
import java.io.IOException;
import java.util.*;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("Duplicates")
public class HURFileExecutor extends HURFileProcessor {

    private static final Logger logger = LoggerFactory.getLogger(HURFileExecutor.class);
    private Gson gson;
    private List<LinkedHashMap<String, Object>> jsonRecords;
    List<LinkedHashMap<String, Object>> records = new ArrayList<>();

    String[] headers = {"SERIAL_NO", "CALLER_NUMBER", "CALLED_NUMBER", "FORWARDED_NUMBER", "RECORD_TYPE", "DURATION", "TIME_STAMP", "EQUIPMENT_ID", "IMSI_NUMBER", "GEOGRAPHIC_POSITION", "CALL_TYPE", "VALUE", "CDR_TYPE", "SERVICE_TYPE", "IS_COMPLETE", "IS_ATTEMPTED", "SERVICE", "PHONE_NUMBER", "VPMN"};

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

        DelimitedFileDecoder decoder = null;
        try {
            long recCount = 1;

            decoder = new DelimitedFileDecoder(fileName, ',', headers, 11);
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

    public boolean hasNext() throws IOException {
        return records.size() > 0;
    }

    public LinkedHashMap<String, Object> next() throws Exception {
        return records.remove(9);
    }

//    private void parseRecords(String fileName) {
//        String cvsSplitBy = ",";
//        int numOfLinesToSkip = 9; // number of lines to skip at the beginning
//        List<String> headerVal = new ArrayList<>();
//        List<List<String>> dataVal = new ArrayList<>();
//        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
//            List<String> linesToSkip = br.lines()
//                    .limit(numOfLinesToSkip)
//                    .collect(Collectors.toList());
//            headerVal = Arrays.asList(br.readLine().split(cvsSplitBy));
//            dataVal = br.lines()
//                    .map(line -> Arrays.asList(line.split(cvsSplitBy)))
//                    .filter(values -> values.size() > 1) // filter out lines with fewer than 2 values
//                    .map(values -> values.subList(0, values.size()))
//                    .collect(Collectors.toList());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        createRecords(headerVal, dataVal);
//    }
//
//    private void createRecords(List<String> headerVal, List<List<String>> dataVal) {
//       // String[] headers = new String[headerVal.size() + dataVal.get(0).size()]; // initialize headers array with the correct size
////        headerVal.toArray(headers); // copy the header values to the headers array
//        for (List<String> d : dataVal) {
//            LinkedHashMap<String, Object> record = new LinkedHashMap<>();
//            for (int i = 0; i < headerVal.size(); i++) {
//                record.put(headers[i], headerVal.get(i));
//            }
//            for (int j = 0; j < d.size(); j++) {
//                record.put(headers[j], d.get(j));
//            }
//            records.add(record);
//        }
//    }

    private void processRecord(LinkedHashMap<String, Object> record, IEnrichment enrichment) throws Exception {
        if (enrichment != null) {
            MEnrichmentReq request = new MEnrichmentReq();
            request.setRequest(record);
            MEnrichmentResponse response = enrichment.transform(request);
            if (response.isResponseCode()) {
                LinkedHashMap<String, Object> data = response.getResponse();
                metadata.noOfParsedRecord++;
                if (data != null)
                    handleEvents("HUR_FCT", data);
            }
        }
    }

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }
}
