package com.gamma.skybase.build.server.etl.tx.tap.rated;

import com.gamma.components.commons.DateUtility;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.telco.OpcoBusinessTransformation;
import com.gamma.telco.opco.ReferenceDimDialDigit;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;

public class RatedTapoutRecordEnrichment implements IEnrichment {

    private static final Logger logger = LoggerFactory.getLogger(RatedTapoutRecordEnrichment.class);
    private static final String TARGET_FORMAT = "yyyyMMdd HH:mm:ss";
    private static final String SOURCE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final OpcoBusinessTransformation transformationLib = new OpcoBusinessTransformation();

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        LinkedHashMap<String, Object> output = new LinkedHashMap<>(data);
        try{
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TARGET_FORMAT).withZone(ZoneId.systemDefault());
            String eventDate = String.valueOf(data.get("event_date"));
            Date transDate = DateUtility.convertString2JavaUtilDate(eventDate, SOURCE_FORMAT);
            String chargeTimestamp = String.valueOf(data.get("start_time_charge_timestamp"));
            Date chargeTimestampDate = DateUtility.convertString2JavaUtilDate(chargeTimestamp, SOURCE_FORMAT);
            output.put("event_date", formatter.format(transDate.toInstant()));
            output.put("start_time_charge_timestamp", formatter.format(chargeTimestampDate.toInstant()));
            output.put("XDR_DATE", formatter.format(transDate.toInstant()));
            output.put("POPULATION_DATE", formatter.format(Instant.now()));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null; /* skipping further processing if txn_date missing */
        }

        String callType = String.valueOf(data.get("call_type"));
        String tariffInfoSncode = String.valueOf(data.get("tariff_info_sncode"));

        String eventTypeKey = "-99";
        String eventDirectionKey = "-99";

        switch (callType){
            case "1":   // voice
            case "8":   // call forwards
                eventTypeKey = "1";
                if ("1".equalsIgnoreCase(tariffInfoSncode)){    //outgoing
                    eventDirectionKey = "1";
                }else if ("2".equalsIgnoreCase(tariffInfoSncode)){  //incoming
                    eventDirectionKey = "2";
                }
                break;
            case "2":   //sms
                eventTypeKey = "2";
                if ("1".equalsIgnoreCase(tariffInfoSncode)){    //outgoing
                    eventDirectionKey = "1";
                }
                break;
            case "11":
                eventTypeKey = "4"; //gprs
                break;
            case "13":
                eventTypeKey = "3"; //mms
                break;
        }
        output.put("EVENT_TYPE_KEY", eventTypeKey);
        output.put("EVENT_DIRECTION_KEY", eventDirectionKey);

        /* handling numeric fields */
        List<String> numeric = Arrays.asList("duration_volume", "rated_volume", "rounded_volume", "zero_rounded_volume_volume",
                "eff_chrg_inf_xcurr_disc_amount", "eff_chrg_inf_hcurr_disc_amount", "eff_charge_info_xcurr_tax");
        for (String item : numeric) {
            try {
                output.put(item, Double.parseDouble((String) data.get(item)));
            } catch (Exception e) {
                output.put(item, 0D);
            }
        }

        //s_p_number_address - served
        String servedMsisdn = String.valueOf(data.get("s_p_number_address"));
        ReferenceDimDialDigit sddk = transformationLib.getDialedDigitSettings(servedMsisdn);
        output.put("SERVED_MSISDN", servedMsisdn);
        if (sddk != null) {
            output.put("SERVED_MSISDN_DIAL_DIGIT", sddk.getDialDigitKey());
            output.put("SERVED_MSISDN_ISO_CODE", sddk.getIsoCountryCode());
            output.put("SERVED_MSISDN_OPER", sddk.getProviderDesc());
            output.put("PARTNER_OPER", sddk.getProviderDesc());
            output.put("PARTNER_COUNTRY_ISO", sddk.getIsoCountryCode());
            output.put("PARTNER_COUNTRY", sddk.getIsoCountryCode());
        }

        //o_p_normed_num_address - other
        Object opNormedNumAddress = data.get("o_p_normed_num_address");
        if (opNormedNumAddress != null) {
            String otherMsisdn = String.valueOf(opNormedNumAddress);
            if (StringUtils.isNotEmpty(otherMsisdn)) {
                ReferenceDimDialDigit oddk = transformationLib.getDialedDigitSettings(otherMsisdn);
                output.put("OTHER_MSISDN", otherMsisdn);
                if (oddk != null) {
                    output.put("OTHER_MSISDN_DIAL_DIGIT", oddk.getDialDigitKey());
                    output.put("OTHER_MSISDN_ISO_CODE", oddk.getIsoCountryCode());
                    output.put("OTHER_MSISDN_OPER", oddk.getProviderDesc());
                }
            }
        }
        return output;
    }

    @Override
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
