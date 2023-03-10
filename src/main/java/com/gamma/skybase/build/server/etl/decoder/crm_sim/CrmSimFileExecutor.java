package com.gamma.skybase.build.server.etl.decoder.crm_sim;

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

public class CrmSimFileExecutor extends CrmSimFileProcessor {

    private static final Logger logger = LoggerFactory.getLogger(CrmSimFileExecutor.class);
    private List<LinkedHashMap<String, Object>> jsonRecords;

    String[] headers = new String[]{"RES_ID", "RES_CODE", "VENDER_ID", "MODEL_ID", "CATEGORY_ID", "DNSEQ", "CREATE_DATE", "VALID_DATE", "INVALID_DATE", "DEPT_ID", "PERSON", "LEVEL_ID", "IS_BIND", "PACKAGE_MODE", "PACKAGE_ID", "RES_STATUS_ID", "IS_LOCKED", "IS_RECYCLED", "OPER_DATE", "OPER_ID", "ORDER_STATUS", "ICCID", "IMSI", "K", "PIN1", "PUK1", "PIN2", "PUK2", "KLA1", "KLA2", "KLA3", "KIC3", "KID3", "KIC8", "KID8", "KIC9", "KID9", "HLR_CODE", "LOCAL_LAN", "LAN_HLR", "PAYMENT_MODE", "BATCH_ID", "LOTID", "WARRANTY_PERIOD", "TELE_TYPE", "ESN", "AKEY", "MDN_TYPE", "IMSI2", "PREFIX", "PRICE", "CITY", "HLR_NEW", "KI", "ADM1", "KOTA", "BE_ID", "BRAND_ID"
    };

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

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

        DelimitedFileDecoder decoder = null;
        try {
            long recCount = 0;

            decoder = new DelimitedFileDecoder(fileName, '|', headers, 0);
            while (decoder.hasNext()) {
                try {
                    LinkedHashMap<String, Object> record = decoder.next();
                    if (jsonOutputRequired) jsonRecords.add(record);

                    record.put("fileName", metadata.decompFileName);
                    processRecord(record, enrichment);
                } catch (Exception e) {
                    e.printStackTrace();
                    metadata.comments = "Parsing issues";
                    metadata.noOfBadRecord++;
                    logger.error(dataSource.getAdapterName() + " -> ", e.getMessage(), e);
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
                    handleEvents("FCT_CRM_SIM", data);
            }
        }
    }
}
