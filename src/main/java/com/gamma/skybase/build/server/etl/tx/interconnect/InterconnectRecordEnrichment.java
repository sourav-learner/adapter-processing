package com.gamma.skybase.build.server.etl.tx.interconnect;

import com.gamma.components.commons.DateUtility;
import com.gamma.skybase.build.utility.MTrunkGroup;
import com.gamma.skybase.build.utility.TrunkGroupConfigManager;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InterconnectRecordEnrichment implements IEnrichment {

    private static final Logger logger = LoggerFactory.getLogger(InterconnectRecordEnrichment.class);
    private static final String TARGET_FORMAT = "yyyyMMdd HH:mm:ss";
    private static final String SOURCE_FORMAT = "yyyy-MM-dd";
    public static final String tgPrefixRegex = "(.+)\\d*";
    public static final Pattern tgPrefixPattern = Pattern.compile(tgPrefixRegex);
    public static final String[] tgPrefixFields = {"tgPrefix", "tgId"};

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        LinkedHashMap<String, Object> output = new LinkedHashMap<>(data);
        try{
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TARGET_FORMAT).withZone(ZoneId.systemDefault());
            String eventDate = String.valueOf(data.get("event_date"));
            Date transDate = DateUtility.convertString2JavaUtilDate(eventDate, SOURCE_FORMAT);
            output.put("XDR_DATE", formatter.format(transDate.toInstant()));
            output.put("POPULATION_DATE", formatter.format(Instant.now()));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null; /* skipping further processing if txn_date missing */
        }

        String callType = String.valueOf(data.get("call_type"));
        String tariffInfoSncode = String.valueOf(data.get("tariff_info_sncode"));
        String tariffInfoUsageInd = String.valueOf(data.get("tariff_info_usage_ind"));

        String eventTypeKey = "-99";

        switch (tariffInfoSncode){
            case "1":
                if ("1".equalsIgnoreCase(callType) || "8".equalsIgnoreCase(callType)){  //outgoing voice & call forwards
                    eventTypeKey = "1";
                }else if ("2".equalsIgnoreCase(callType)){  //incoming voice call
                    eventTypeKey = "1";
                }else if ("4".equalsIgnoreCase(callType)){  //which part is this ???
                    eventTypeKey = "-99";
                }
                break;
            case "2":
                if ("1".equalsIgnoreCase(callType)){    //outgoing sms
                    eventTypeKey = "2";
                }
                break;
        }
        output.put("EVENT_TYPE_KEY", eventTypeKey);

        /* handling numeric fields */
        List<String> numeric = Arrays.asList("calls_count","rated_flat_amount","duration_volume");
        for (String item : numeric) {
            try {
                output.put(item, Double.parseDouble((String) data.get(item)));
            } catch (Exception e) {
                output.put(item, 0D);
            }
        }

        /* NW_IND_KEY enrichment */
        String infoCustomerId = String.valueOf(data.get("cust_info_customer_id"));
        if (Arrays.asList("733","727","719","1207").contains(infoCustomerId)){
            output.put("NW_IND_KEY", "2");
        }else {
            output.put("NW_IND_KEY", "3");
        }

        /* enrich trunk group via trunk prefix */
        String tgip = String.valueOf(data.get("trunk_group_in_address"));
        String tgop = String.valueOf(data.get("trunk_group_out_address"));

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
        output.put("IN_TRUNK", iTrunk);
        output.put("OUT_TRUNK", oTrunk);

        /* revenue & cost CDRs identification */
        String tag = "";
        MTrunkGroup oto = TrunkGroupConfigManager.instance().getTrunkGroupMap().get(oTrunk);
        if (Arrays.asList("2", "4").contains(callType)
            && Arrays.asList("3", "38").contains(tariffInfoUsageInd)
            && oto != null
            && Arrays.asList("S", "Z").contains(oto.getType())){
            tag = "revenue";
        }

        MTrunkGroup otpo = TrunkGroupConfigManager.instance().getTrunkGroupPrefix1Map().get(tgop);
        if (StringUtils.isEmpty(tag)
            && Arrays.asList("1", "4").contains(callType)
            && Arrays.asList("2", "39").contains(tariffInfoUsageInd)
            && otpo != null
            && !Arrays.asList("S", "Z").contains(otpo.getType())){
            tag = "cost";
        }
        output.put("TAGS", tag);
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
