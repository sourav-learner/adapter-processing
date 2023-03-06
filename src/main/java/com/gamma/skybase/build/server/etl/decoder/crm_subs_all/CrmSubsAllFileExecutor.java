package com.gamma.skybase.build.server.etl.decoder.crm_subs_all;

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
public class CrmSubsAllFileExecutor extends CrmSubsAllFileProcessor {
    private static final Logger logger = LoggerFactory.getLogger(CrmSubsAllFileExecutor.class);
    private List<LinkedHashMap<String, Object>> jsonRecords;
    String[] headers = new String[]{"SUB_ID" , "CUST_ID" , "ACTUAL_CUST_ID" , "SUB_TYPE" , "SUBGROUP_TYPE" , "SUBGROUP_NAME" , "NETWORK_TYPE" , "PREPAID_FLAG" , "MSISDN" , "IMSI" , "ICCID" , "SUB_PASSWORD" , "SUB_LAN" , "SUB_LEVEL" , "SUB_GROUP" , "SUB_SEGMENT" , "DUN_FLAG" , "DUN_START_DATE" , "DUN_EXPIRY_DATE" , "SUB_INIT_CREDIT" , "SUB_CREDIT" , "SUB_STATE" , "SUB_STATE_REASON" , "AGREEMENT_NO" , "CREATE_DATE" , "AGREEMENT_DATE" , "FIRST_EFF_DATE" , "EFF_DATE" , "EXP_DATE" , "MOD_DATE" , "ACTIVE_DATE" , "LATEST_ACTIVE_DATE" , "CREATE_OPER_ID" , "CREATE_LOCAL_ID" , "BUSI_SEQ" , "REMARK" , "PARTITION_ID" , "SUB_INIT_CREDIT_ID" , "VIRTUAL_SUB_ID" , "CPE_MAC" , "RESERVE_RECONN_TIME" , "DEALER_ID" , "INFO1" , "INFO2" , "INFO3" , "INFO4" , "INFO5" , "INFO6" , "INFO7" , "INFO8" , "INFO9" , "INFO10" , "INFO11" , "INFO12" , "RELA_MSISDN" , "USERNAME" , "BRAND_ID" , "DISPLAY_TYPE" , "BE_ID" , "SALES_OPER_ID" , "SALES_DEPT_ID" , "SALES_DEALER_ID" , "SALES_CHANNEL" , "CREATE_DEPT_ID" , "PARENT_SUB_ID" , "EXTERNAL_SUB_ID" , "SUB_CODE" , "WRITTEN_LAN" , "OWNER_ORG_ID" , "SN_TYPE"};

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
                    handleEvents("FCT_HUW_CRM_SUBS_ALL", data);
            }
        }
    }

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }
}