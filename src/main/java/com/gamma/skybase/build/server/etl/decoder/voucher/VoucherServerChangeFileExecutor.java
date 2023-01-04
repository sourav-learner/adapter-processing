package com.gamma.skybase.build.server.etl.decoder.voucher;

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

import static com.gamma.components.commons.constants.GammaConstants.PATH_SEP;

/**
 * Created by abhi on 13/05/20
 */
public class VoucherServerChangeFileExecutor extends VoucherServerChangeFileProcessor {
    private static final Logger logger = LoggerFactory.getLogger(VoucherServerChangeFileExecutor.class);
    private List<LinkedHashMap<String, Object>> jsonRecords;

    @Override
    @SuppressWarnings("Duplicates")
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
        //SERIAL_NO,STATUS,TIMESTAMP,EXIPARY_DATE,VALUE,SUBSCRIBER_ID,OPERATOR_ID,CURRENCY,GENERATED_FULL_DATE,INSERTDATE,ORIGINAL_FILE_NAME
        String[] headers = {"SERIAL_NO", "STATUS", "TIMESTAMP", "EXPIRY_DATE", "VALUE", "SUBSCRIBER_ID", "OPERATOR_ID", "CURRENCY",
                "GENERATED_FULL_DATE", "INSERTDATE", "ORIGINAL_FILE_NAME"};
        try (DelimitedFileDecoder decoder = new DelimitedFileDecoder(fileName, ',', headers, 1)) {
            logger.info("Adapter {} decoding File {} ", dataSource.getAdapterName(), new File(fileName).getName());
            long recCount = 0;
            while (decoder.hasNext()) {
                try {
                    LinkedHashMap<String, Object> record = decoder.next();
                    record.put("fileName", this.metadata.decompFileURL.substring(this.metadata.decompFileURL.lastIndexOf(File.separator) + 1));
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
            FileWriter writer = new FileWriter("out" + PATH_SEP + dataSource.getAdapterName() + PATH_SEP + fn + ".json");
            writer.write(gson.toJson(jsonRecords));
            writer.flush();

        }
    }

    private void processRecord(LinkedHashMap<String, Object> record, IEnrichment enrichment) {
        try {
            if (enrichment != null) {
                MEnrichmentReq request = new MEnrichmentReq();
                request.setRequest(record);
                MEnrichmentResponse response = enrichment.transform(request);
                LinkedHashMap<String, Object> data = response.getResponse();
                handleEvents("VS_CHANGE_FCT", data);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }
}
