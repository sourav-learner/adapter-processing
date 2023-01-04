package com.gamma.skybase.build.server.etl.decoder.hur;

import com.gamma.components.commons.constants.GammaConstants;
import com.gamma.components.structure.IDatum;
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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@SuppressWarnings("Duplicates")
public class HURFileExecutor extends HURFileProcessor {

    private static final Logger logger = LoggerFactory.getLogger(HURFileExecutor.class);
    private Gson gson;
    private List<LinkedHashMap<String, Object>> jsonRecords;
    List<LinkedHashMap<String, Object>> records = new ArrayList<>();
    String[] headers = {"SENDER", "RECIPIENT", "SEQUENCE_NO", "THRESHOLD", "REPORT_ANALYSIS_TIME", "REPORT_CREATION_TIME", "IMSI", "DATE_FIRST_EVENT", "TIME_FIRST_EVENT", "DATE_LAST_EVENT", "TIME_LAST_EVENT", "DC_HHMMSS", "NC", "VOLUME", "SDR"};

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

        parseRecords(fileName);

        long recCount = 0;
        while (hasNext()) {
            LinkedHashMap<String, Object> record = next();
            record.put("fileName", this.metadata.decompFileURL.substring(this.metadata.decompFileURL.lastIndexOf(File.separator) + 1));
            record.put("fileDate", this.metadata.srcFileDate);
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

        if (jsonOutputRequired) {
            FileWriter writer = new FileWriter("out" + GammaConstants.PATH_SEP + fn + ".json");
            writer.write(gson.toJson(jsonRecords));
            writer.flush();
        }
    }

    public boolean hasNext() throws IOException {
        return records.size() > 0;
    }

    public LinkedHashMap<String, Object> next() throws Exception {
        return records.remove(0);
    }

    private void parseRecords(String fileName) {
        List<String> headerVal = new ArrayList<>();
        List<List<String>> dataVal = new ArrayList<>();
        try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
            Long count = stream.map(e -> e.split(","))
                    .filter(e -> e.length > 5)
                    .peek(e -> {
                        if (e[0].trim().equals("H"))
                            headerVal.addAll(Arrays.asList(e).subList(1, e.length));
                    })
                    .peek(e -> {
                        if (e[0].trim().equals("C"))
                            dataVal.add(Arrays.asList(e).subList(1, e.length));
                    }).count();
            System.out.println(count);

        } catch (IOException e) {
            e.printStackTrace();
        }
        createRecords(headerVal, dataVal);
    }

    private void createRecords(List<String> headerVal, List<List<String>> dataVal) {
        dataVal.forEach(d -> {
            LinkedHashMap<String, Object> record = new LinkedHashMap<>();
            for (String header : headers) record.put(header, "");

            AtomicInteger i = new AtomicInteger();
            headerVal.forEach(h -> {
                record.put(headers[i.get()], h);
                i.getAndIncrement();
            });

            for (int j = 0; j < d.size(); j++)
                record.put(headers[headerVal.size() + j], d.get(j));

            records.add(record);

        });
    }

    private void processRecord(LinkedHashMap<String, Object> record, IEnrichment enrichment) throws Exception {
        if (enrichment != null) {
            MEnrichmentReq request = new MEnrichmentReq();
            request.setRequest(record);
            MEnrichmentResponse response = enrichment.transform(request);
            LinkedHashMap<String, Object> data = response.getResponse();
            handleEvents("HUR_FCT", data);
        }
    }

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }
}
