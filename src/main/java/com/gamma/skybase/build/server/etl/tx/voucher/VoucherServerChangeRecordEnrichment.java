package com.gamma.skybase.build.server.etl.tx.voucher;

import com.gamma.components.commons.AppUtility;
import com.gamma.components.commons.DateUtility;
import com.gamma.components.commons.app.AppConfig;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.LinkedHashMap;

import static com.gamma.components.commons.AppUtility.getValueOrDefault;

/**
 * Created by abhi on 14/05/20
 */
public class VoucherServerChangeRecordEnrichment implements IEnrichment {

    private static final int VS_DIVIDE_FACTOR = 100;
    private static final Logger logger = LoggerFactory.getLogger(VoucherServerChangeRecordEnrichment.class);
    private static final String COUNTRY_CODE = AppConfig.instance().getProperty("app.datasource.countrycode");
    private static final String TARGET_FORMAT = "yyyyMMdd HH:mm:ss";

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        LinkedHashMap<String, Object> output = new LinkedHashMap<>();
        try {
            Date voucherTimestamp = null;
            Date expiryDate;
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TARGET_FORMAT).withZone(ZoneId.systemDefault());
            //SERIAL_NO,STATUS,TIMESTAMP,EXIPARY_DATE,VALUE,SUBSCRIBER_ID,OPERATOR_ID,CURRENCY,GENERATED_FULL_DATE,INSERTDATE,ORIGINAL_FILE_NAME
            output.put("SERIAL_NO", getValueOrDefault(data.get("SERIAL_NO")));
            output.put("STATUS", getValueOrDefault(data.get("STATUS")));
            String subscriberId = getValueOrDefault(data.get("SUBSCRIBER_ID"));
            output.put("SUBSCRIBER_ID", subscriberId);
            if(!subscriberId.isEmpty() && !subscriberId.startsWith(COUNTRY_CODE)) {
                subscriberId = COUNTRY_CODE + subscriberId;
                output.put("SERVED_MSISDN", subscriberId);
            }
            if(data.get("TIMESTAMP") != null){
                //20190226T01:06:36
                voucherTimestamp = DateUtility.convertString2JavaUtilDate(getValueOrDefault(data.get("TIMESTAMP")),
                        "yyyyMMdd'T'HH:mm:ss");
                output.put("VOUCHER_TIMESTAMP", formatter.format(voucherTimestamp.toInstant()));
            }
            if(data.get("EXPIRY_DATE") != null) output.put("EXPIRY_DATE", data.get("EXPIRY_DATE"));

            if (data.get("VALUE") instanceof Double){
                output.put("VOUCHER_VALUE", ((Double) data.get("VALUE"))/VS_DIVIDE_FACTOR);
            } else if (data.get("VALUE") instanceof String){
                try{
                    output.put("VOUCHER_VALUE", Double.parseDouble((String) data.get("VALUE"))/VS_DIVIDE_FACTOR);
                }catch (Exception e){
                    output.put("VOUCHER_VALUE", 0);
                }
            }

            output.put("OPERATOR_ID", getValueOrDefault(data.get("OPERATOR_ID")));
            output.put("CURRENCY", getValueOrDefault(data.get("CURRENCY")));
            output.put("POPULATION_DATE_TIME", formatter.format(Instant.now()));
            output.put("FILE_NAME", getValueOrDefault(data.get("fileName")));
            if (voucherTimestamp != null) output.put("XDR_DATE", formatter.format(voucherTimestamp.toInstant()));

            return output;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    @Override
    @SuppressWarnings("Duplicates")
    public MEnrichmentResponse transform(MEnrichmentReq request) {
        LinkedHashMap<String, Object> data = transform(request.getRequest());
        MEnrichmentResponse response = new MEnrichmentResponse();
        if(data != null) {
            response.setResponseCode(true);
            response.setResponse(data);
        }
        else response.setResponseCode(false);
        return response;
    }
}
