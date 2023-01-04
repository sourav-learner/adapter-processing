package com.gamma.skybase.build.server.etl.tx.interconnect;

import com.gamma.components.commons.DateUtility;
import com.gamma.components.commons.app.AppConfig;
import com.gamma.skybase.build.utility.IntlZoneConfigManager;
import com.gamma.skybase.build.utility.MIntlZone;
import com.gamma.skybase.build.utility.TrunkGroupConfigManager;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.telco.OpcoBusinessTransformation;
import com.gamma.telco.opco.ReferenceDimDialDigit;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;

public class InterconnectMarginRecordEnrichment implements IEnrichment {

    private static final Logger logger = LoggerFactory.getLogger(InterconnectMarginRecordEnrichment.class);
    private static final String TARGET_FORMAT = "yyyyMMdd HH:mm:ss";
    private final String countryCode = AppConfig.instance().getProperty("app.datasource.countrycode");
    private final ThreadLocal<SimpleDateFormat> targetDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat(TARGET_FORMAT));
    private static final OpcoBusinessTransformation transformationLib = new OpcoBusinessTransformation();

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        LinkedHashMap<String, Object> record = new LinkedHashMap<>(data);

        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TARGET_FORMAT).withZone(ZoneId.systemDefault());
            Object et = record.get("event_time");
            if (et != null) {
                String fileDateTxt = String.valueOf(et);
                Date fileDate = DateUtility.convertWideString2JavaUtilDate(fileDateTxt);
                String recordTimeTxt = targetDateFormat.get().format(fileDate);
                record.put("XDR_DATE", recordTimeTxt);
                record.put("event_time", recordTimeTxt);
            }

            Object edt = record.get("entry_date_timestamp");
            if (edt != null) {
                String entryDateTxt = String.valueOf(edt);
                Date entryDate = DateUtility.convertWideString2JavaUtilDate(entryDateTxt);
                String recordTimeTxt = targetDateFormat.get().format(entryDate);
                record.put("entry_date_timestamp", recordTimeTxt);
            }
            record.put("POPULATION_DATE", formatter.format(Instant.now()));

            /* handling numeric fields */
            List<String> numeric = Arrays.asList("rated_flat_amount","duration_volume");
            for (String item : numeric) {
                try {
                    record.put(item, Double.parseDouble((String) record.get(item)));
                } catch (Exception e) {
                    record.put(item, 0D);
                }
            }

            /* MSISDN enrichment */
            String msisdn = String.valueOf(data.get("served_msisdn"));
            if (StringUtils.isNotEmpty(msisdn) && !msisdn.startsWith(countryCode)) {
                msisdn = countryCode + msisdn;
            }
            record.put("served_msisdn", msisdn);

            /* enrich trunk group via trunk prefix */
            String tgip = String.valueOf(record.get("trunk_group_in_address"));
            String tgop = String.valueOf(record.get("trunk_group_out_address"));

            String iTrunk = tgip;
            String oTrunk = tgop;
            if (TrunkGroupConfigManager.instance().getTrunkGroupPrefix1Map().containsKey(tgip)){
                iTrunk = TrunkGroupConfigManager.instance().getTrunkGroupPrefix1Map().get(tgip).getId();
            }else {
                logger.info("Trunk group prefix mapping not found - {}", tgip);
                iTrunk = tgip.replaceAll("\\d*$", "");
            }
            if (TrunkGroupConfigManager.instance().getTrunkGroupPrefix1Map().containsKey(tgop)){
                oTrunk = TrunkGroupConfigManager.instance().getTrunkGroupPrefix1Map().get(tgop).getId();
            }else {
                logger.info("Trunk group prefix mapping not found - {}", tgop);
                oTrunk = tgop.replaceAll("\\d*$", "");
            }
            record.put("IN_TRUNK", iTrunk);
            record.put("OUT_TRUNK", oTrunk);

            double dv = (Double) record.get("duration_volume");
            double rfa = (Double) record.get("rated_flat_amount");
            long bp = (long) Math.ceil(dv / 60);
            record.put("BILLABLE_PULSE", bp);

            /* dial digit analysis */
            String servedMsisdn = String.valueOf(record.get("other_msisdn"));
            ReferenceDimDialDigit sddk = transformationLib.getDialedDigitSettings(servedMsisdn);
            if (sddk != null) {
                record.put("PARTNER_DDK", sddk.getDialDigitKey());
                record.put("PARTNER_COUNTRY", sddk.getIsoCountryCode());
                record.put("PARTNER_OPER", sddk.getProviderDesc());

                String dialCode = sddk.getTargetCountryCode();
                record.put("DIAL_CODE", dialCode);

                /* min_pulse_ind, chargeable_unit_sec, avg_tariff using dial_code*/
                MIntlZone zone = IntlZoneConfigManager.instance().get(dialCode);
                if (zone != null){
                    if ("true".equalsIgnoreCase(zone.getMinPulse())) record.put("MIN_PULSE", "1");
                    else record.put("MIN_PULSE", "0");
                    record.put("ZONE", zone.getZone());
                    if ("true".equalsIgnoreCase(zone.getMinPulse())){
                        record.put("CHARGEABLE_UNITS", bp);
                        if (bp > 0) record.put("AVG_TARIFF", rfa/bp);
                    }else {
                        record.put("CHARGEABLE_UNITS", dv);
                        if (dv > 0) record.put("AVG_TARIFF", rfa/dv);
                    }
                }else {
                    logger.info("[Interconnect Margin] Urgent action required. Dial Code Mapping not found for - {}", dialCode);
                }
            }else {
                logger.info("[Interconnect Margin] Urgent action required. Dial Digit Mapping not found for - {}", servedMsisdn);
            }
        }catch (Exception ignored){}
        return record;
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
