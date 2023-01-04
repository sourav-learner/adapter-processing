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
import java.util.*;

import static com.gamma.components.commons.constants.GammaConstants.PATH_SEP;

/**
 * Created by abhi on 13/05/20
 */
public class VoucherServerDumpFileExecutor extends VoucherServerDumpFileProcessor{
    private static final Logger logger = LoggerFactory.getLogger(VoucherServerDumpFileExecutor.class);
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
        } catch (ClassNotFoundException |InstantiationException |IllegalAccessException e) {
            logger.error(e.getMessage(), e);
        }

        /**
         * State,SubState,VoucherGroup,Value(MRP),Agent,Currency,Voucher Creation Date,Voucher Expiry Date and Time,
         * Voucher State Change Date and Time,Created By,Changed By,Activation Code,Serial No,Subscriber Id,Transaction Id,
         * Batch Id,Extension Text1,Extension Text2,Extension Text3,Supplier Id,Dynamic Attribute 1,Dynamic Attribute 2,
         * Dynamic Attribute 3,Dynamic Attribute 4,Dynamic Attribute 5,Dynamic Attribute 6,Dynamic Attribute 7,Dynamic Attribute 8,
         * Dynamic Attribute 9,Dynamic Attribute 10,Record Type(C/H)
         */

        String [] headers = {"State", "SubState", "VoucherGroup", "Value", "Agent", "Currency", "VoucherCreationDate",
            "VoucherExpiryDateTime", "VoucherStateChangeDate", "CreatedBy", "ChangedBy", "SerialNo", "SubscriberId",
            "TransactionId", "BatchId", "ExtensionText1", "ExtensionText2", "ExtensionText3", "SupplierId", "DynamicAttribute1",
            "DynamicAttribute2", "DynamicAttribute3", "DynamicAttribute4", "DynamicAttribute5", "DynamicAttribute6",
            "DynamicAttribute7", "DynamicAttribute8", "DynamicAttribute9", "DynamicAttribute10", "RecordType"
        };
        try (DelimitedFileDecoder decoder = new DelimitedFileDecoder(fileName, ',', headers, 1)){
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
        if (jsonOutputRequired){
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
                handleEvents("VS_DUMP_FCT", data);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }
}
