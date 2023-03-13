package com.gamma.skybase.build.server.etl.decoder.crm_dealer;

import com.gamma.skybase.build.server.etl.decoder.LebaraUtil;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CrmDealerEnrichmentUtil extends LebaraUtil{

    private CrmDealerEnrichmentUtil(LinkedHashMap<String, Object> record) {
        super(record);
    }

    public static CrmDealerEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new CrmDealerEnrichmentUtil(record);
    }

    public Optional<String> getDealerStatus() {
        String dealerStatus = null;
        String status = getValue("STATUS");
        if (status != null) {
            switch (status) {
                case "0":
                    dealerStatus = "VALID";
                    break;
                case "1":
                    dealerStatus = "INVALID";
                    break;
                default:
                    dealerStatus = "-99";
                    break;
            }
        }
        if (dealerStatus != null)
            return Optional.of(dealerStatus);
        return Optional.empty();
    }

    public Optional<String> getCreateDate() {
        String createDate;
        String createDate1;

        createDate1 = getValue("CREATE_DATE");
        if (createDate1 != null) {
            try {
                SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMdd");
                Date date = inputFormat.parse(createDate1);

                SimpleDateFormat outputFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
                createDate = outputFormat.format(date);
                return Optional.of(createDate);
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
        return Optional.empty();
    }

    public Optional<String> getEventDate(){
        String fileName;
        fileName = getValue("fileName");
        String eventDate = null;
        Pattern pattern = Pattern.compile("^[A-Za-z0-9_]+_[A-Za-z0-9_]+_([0-9]{8})\\.[A-Za-z]+$");
        Matcher matcher = pattern.matcher(fileName);
        if (matcher.find()) {
            eventDate = matcher.group(1);
        }
        if (eventDate != null){
            return Optional.of(eventDate);
        }
        return Optional.empty();
    }
}