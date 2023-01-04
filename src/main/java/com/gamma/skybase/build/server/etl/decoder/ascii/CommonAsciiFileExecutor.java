package com.gamma.skybase.build.server.etl.decoder.ascii;

import com.gamma.components.commons.app.AppConfig;
import com.gamma.components.structure.IDatum;
import com.gamma.decoder.ascii.DelimitedFileDecoder;
import com.gamma.skybase.build.server.etl.utils.SDPLookupUpdate;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.skybase.contract.decoders.SkybaseDeserializerException;
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

public class CommonAsciiFileExecutor extends CommonAsciiFileProcessor {
    private static final Logger logger = LoggerFactory.getLogger(CommonAsciiFileExecutor.class);
    private List<LinkedHashMap<String, Object>> jsonRecords;
    private SDPLookupUpdate sdpLookupUpdate;

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

        boolean fileHasHeader = true;
        char columnSeparator = ',';
        String[] headers = null;
        String adapter = dataSource.getAdapterName().toLowerCase();
        String msisdnKeyField = "";
        switch (adapter) {
            case "roaming-subscriber":
                fileHasHeader = true;
                headers = new String[]{"dn_num", "serv", "start_date", "sncode", "status"};
                msisdnKeyField = "dn_num";
                break;
            case "cbio-subscriber":
                fileHasHeader = true;
                headers = new String[]{"dn_num", "co_id", "sm_serialnum", "port_num", "dealer", "port_assign_date", "cd_status", "cd_pending_state", "cd_seqno", "tmcode", "service_class_id", "external_key", "des", "used_context"};
                msisdnKeyField = "dn_num";
                break;
            case "erp":
                fileHasHeader = true;
                headers = new String[]{"Insert Date","Trans Date","Serviceclass","Finalcharge Wo Tax","Charged Duration","Revenue Type"};
                break;
            case "msdp":
                fileHasHeader = false;
                columnSeparator = '|';
                headers = new String[]{"streamno", "timestamp", "chargenodeid", "msgid", "original msisdn", "oaoperatorid", "destination msisdn", "daoperatorid", "cdrtype", "msisdn", "spid", "serviceid", "productid", "servicetype", "chargemode", "begintime", "endtime", "times", "fee", "chargeresult", "srcdevicetype", "srcdeviceid", "destdevicetype", "destdeviceid", "paytype", "duration", "volume", "prediscountfee", "bearertype", "bearerprotocoltype", "uplinkvolume", "downlinkvolume", "apn", "roamingoperatorid", "chargingid", "qos", "channelid", "currency", "imsi", "pkgspid", "pkgserviceid", "pkgproductid", "seqnoofslicecdr", "chargepurpose", "msip", "visitedurl/ipaddress", "visitedcategoryid", "accesschannel", "chargetype", "periodictype", "istestcdr", "chargeflow", "servicepaytype", "loyalties", "servicecapability", "voucherid", "iptvuseridentity", "terminalid", "tradeid", "reserved5", "transactionid", "expirydate", "autorenewflag", "cdrcustomizereserve", "rbtvaliddatetime", "chargeby", "sgsnip", "innodeid", "sourceserviceid", "settlementtypeid", "settlementpercent", "bossid", "voucherkey", "campaignid", "userbrand", "inresult", "imei", "isfreeresource", "userlocation", "billedvolume", "billedduration", "mcc-mnc", "rentid", "chargetimes", "actualdebitfee", "sendername", "mostingcampaignid", "chginternalerrorcode", "chgexternalerrorcode", "refundflag", "partialrateflag", "discountflag", "contentprovisonid", "contentprovisontype", "cnfmfailreason", "extendinfo", "feewithouttax", "sessionid"};
                break;
            case "sdp-main":
                sdpLookupUpdate = new SDPLookupUpdate();
                fileHasHeader = false;
                headers = new String[]{"SUBSCRIBERID", "ACCOUNTID", "TEMBLOCKFLAG", "REFILLFAILEDCOUNTER", "REFILLBARENDDATETIME", "FIRSTIVRCALLDONEFLAG", "FIRSTCALLDONEFLAG", "LANGUAGE", "SPECIALANNPLAYEDFLAG", "SERVICEFEEPERIODWARNINGPFLAG", "SUPERVISIONPERIODWARNINGPFLAG", "LOWLEVELWARNINGPLAYEDFLAG", "ORIGINATINGVOICEBLOCKSTATUS", "TERMINATINGVOICEBLOCKSTATUS", "ORIGINATINGSMSBLOCKSTATUS", "TERMINATINGSMSBLOCKSTATUS", "GPRSBLOCKSTATUS", "SERVICECLASSID", "ORIGINALSERVICECLASSID", "TEMSERVICECLASSEXDATE", "ACCOUNTBALANCE", "ACCOUNTACTIVATEDFLAG", "SERVICEFEEEXPIRYDATE", "SUPERVISIONPERIODEXPDATE", "LASTSERVICEFEEDEDDATE", "ACCOUNTDISCONDATE", "SERVICEFEEEXPIRYFLAG", "SERVICEFEEEXPIRYWARNINGFLAG", "CREDITCLEARANCEDATE", "SUPERVISIONEXPIRYFLAG", "SUPERVISIONEXWARNINGFLAG", "NEGATIVEBALANCEBARSDATE", "NEGATIVEBALANCEBARFLAG", "ACCOUNTINEUROFLAG", "ACTIVESERVICEDISABLEDFLAG", "PASSIVESERVICEDISABLEDFLAG", "CONVERGEDFLAG", "LIFECYCLENOTIREPORT", "SERVICEOFFERING", "ACCOUNTGROUPID", "COMMUNITYID1", "COMMUNITYID2", "COMMUNITYID3", "ACCOUNTACTIVATEDDATE", "GLOBAL_ID"};
                msisdnKeyField = "SUBSCRIBERID";
                break;
            case "sdp-ua":
                fileHasHeader = false;
                headers = new String[]{"MSISDN", "UA_ID", "UA_VALUE", "RESET_DATE", "SC_ID"};
                msisdnKeyField = "MSISDN";
                break;
            case "sdp-da":
                fileHasHeader = false;
                headers = new String[]{"MSISDN", "DA_ID", "DA_BALANCE", "DA_EXPIRY_DATE", "EURO_FLAG", "OFFER_ID", "START_DATE", "DA_UNIT_TYPE", "ACC_CATEGORY", "MONEY_UNIT_SUB_TYPE_NUMBER", "DA_ACC_UNIT_BAL_NUMBER", "PAM_SRV_ID", "PRODUCT_ID"};
                msisdnKeyField = "MSISDN";
                break;
            case "sdp-faf":
                fileHasHeader = false;
                headers = new String[]{"MSISDN", "FAF_ID", "COL3", "COL4"};
                msisdnKeyField = "MSISDN";
                break;
            case "sdp-uc":
                fileHasHeader = false;
                headers = new String[]{"MSISDN", "UC_ID", "UC_TYPE", "UC_VALUE", "VALUE_TYPE", "PARTY_ID", "PRODUCT_ID"};
                msisdnKeyField = "MSISDN";
                break;
            case "sdp-ut":
                fileHasHeader = false;
                headers = new String[]{"MSISDN", "UT_ID", "UC_ID", "UT_VALUE", "VALUE_TYPE", "PARTY_ID"};
                msisdnKeyField = "MSISDN";
                break;
            case "sdp-profile":
                fileHasHeader = false;
                headers = new String[]{"SUBSCRIBERID", "ACCOUNTID", "TEMBLOCKFLAG", "REFILLFAILEDCOUNTER", "REFILLBARENDDATETIME", "FIRSTIVRCALLDONEFLAG", "FIRSTCALLDONEFLAG", "LANGUAGE", "SPECIALANNPLAYEDFLAG", "SERVICEFEEPERIODWARNINGPFLAG", "SUPERVISIONPERIODWARNINGPFLAG", "LOWLEVELWARNINGPLAYEDFLAG", "ORIGINATINGVOICEBLOCKSTATUS", "TERMINATINGVOICEBLOCKSTATUS", "ORIGINATINGSMSBLOCKSTATUS", "TERMINATINGSMSBLOCKSTATUS", "GPRSBLOCKSTATUS", "SERVICECLASSID", "ORIGINALSERVICECLASSID", "TEMSERVICECLASSEXDATE", "ACCOUNTBALANCE", "ACCOUNTACTIVATEDFLAG", "SERVICEFEEEXPIRYDATE", "SUPERVISIONPERIODEXPDATE", "LASTSERVICEFEEDEDDATE", "ACCOUNTDISCONDATE", "SERVICEFEEEXPIRYFLAG", "SERVICEFEEEXPIRYWARNINGFLAG", "CREDITCLEARANCEDATE", "SUPERVISIONEXPIRYFLAG", "SUPERVISIONEXWARNINGFLAG", "NEGATIVEBALANCEBARSDATE", "NEGATIVEBALANCEBARFLAG", "ACCOUNTINEUROFLAG", "ACTIVESERVICEDISABLEDFLAG", "PASSIVESERVICEDISABLEDFLAG", "CONVERGEDFLAG", "LIFECYCLENOTIREPORT", "SERVICEOFFERING", "ACCOUNTGROUPID", "COMMUNITYID1", "COMMUNITYID2", "COMMUNITYID3", "ACCOUNTACTIVATEDDATE", "GLOBAL_ID"};
                msisdnKeyField = "SUBSCRIBERID";
                break;
            case "dsp":
                fileHasHeader = true;
                headers = new String[]{"Transaction ID","TeleService Code","Service Provider ID","Service Provider Name","Service Name","Service Type","MSISDN","Service Class ID","Amount","Final Charge","Response Code","Response Message","Day","Time"};
                msisdnKeyField = "MSISDN";
                break;
            case "evd-dealer":
                fileHasHeader = true;
                headers = new String[]{"dealerid","account","max25","max50","max100","billingcycle","receiptno","commissionindex","telephone","imei","imsi","msisdn","email","general","officer","dealertype","totalmonthsale","totalmonthcommission","evouchermsisdn","totalincometax","totalreserved","balance","pin","softwareversion","activitydate","locationid","cellid","arabicname","forcesoftwareupdate","status","zoneindex","lat","lng","bankaccount","cat","subbalance0","subbalance1","subbalance2","subbalance3","subbalance4","subbalance5","subbalance6","subbalance7","subbalance8","subbalance9","bt_index","evids_tid","terminal_serial","carton_order","creation_date","flex_fld1","pin2"};
                break;
            default:
                break;
        }

        DelimitedFileDecoder decoder;
        if (fileHasHeader) {
            decoder = new DelimitedFileDecoder(fileName, columnSeparator, true, false);
        } else {
            decoder = new DelimitedFileDecoder(fileName, columnSeparator, headers, 0);
        }
        try {
            logger.info("Adapter {} decoding File {} ", dataSource.getAdapterName(), new File(fileName).getName());
            long recCount = 0;
            while (decoder.hasNext()) {
                try {
                    LinkedHashMap<String, Object> record = decoder.next();
                    if (jsonOutputRequired) {
                        jsonRecords.add(record);
                    }
                    record.put("fileName", metadata.decompFileName);
                    record.put("fileDate", this.metadata.srcFileDate);
                    if (this.metadata.fileNameAttrib != null) {
                        record.put("fileNode", this.metadata.fileNameAttrib.get("fileNode"));
                    }
                    record.put("node", this.metadata.node);
                    record.put("datasource", this.dataSource.getAdapterName());
                    record.put("_msisdnKeyField", msisdnKeyField);
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
        } catch (Exception e) {
            if (decoder != null) decoder.close();
        }
        if (jsonOutputRequired) {
            FileWriter writer = new FileWriter("out" + PATH_SEP + dataSource.getAdapterName() + PATH_SEP + fn + ".json");
            writer.write(gson.toJson(jsonRecords));
            writer.flush();
        }
    }

    private void processRecord(LinkedHashMap<String, Object> record, IEnrichment enrichment) throws Exception {
        if (enrichment != null) {
            MEnrichmentReq request = new MEnrichmentReq();
            request.setRequest(record);
            MEnrichmentResponse response = enrichment.transform(request);
            if (response.isResponseCode()) {
                LinkedHashMap<String, Object> data = response.getResponse();
                if (data != null) {
                    String eventName = "COMMON_ASCII_STREAM";
                    try {
                        eventName = dataSource.getDataDef().getEvents().values().stream().findFirst().get().getEventName();
                    } catch (Exception ignored) {
                    }

                    /* SDP Specific Code */
                    if ("sdp-main".equalsIgnoreCase(this.dataSource.getAdapterName())) {
                        /* update sdp balance lookup */
                        double balance = Double.parseDouble(String.valueOf(data.getOrDefault("ACCOUNTBALANCE", "0")));
                        String dayId = String.valueOf(data.get("DAY_ID"));
                        String msisdn = String.valueOf(data.get("SUBSCRIBERID"));
                        updateSDPBalanceLookup(msisdn, dayId, balance);
                    }

                    /* generic part */
                    handleEvents(eventName, data);
                }
                metadata.noOfParsedRecord++;
            } else {
                logger.debug("Record enrichment failed.");
            }
        }
    }

    private void updateSDPBalanceLookup(String msisdn, String dayId, double balance) {
        if ("true".equalsIgnoreCase(AppConfig.instance().getProperty("app.sdp-balance-lookup.active", "false"))) {
            try {
                // SkybaseCacheServiceHandler.instance().updateSDPMainBalanceLookup(msisdn, dayId, balance);
                sdpLookupUpdate.updateSDPMainBalanceLookup(msisdn, dayId, balance);
            } catch (Exception ignored) {
            }
        }
    }

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }

    @Override
    public void flush() throws SkybaseDeserializerException {
        if ("sdp-main".equalsIgnoreCase(this.dataSource.getAdapterName())) {
            try {
                // SkybaseCacheServiceHandler.instance().updateSDPMainBalanceLookupEntries();
                sdpLookupUpdate.updateSDPMainBalanceLookupEntries();
            } catch (Exception ignored) {
            }
        }

        /* generic flush */
        super.flush();
    }
}
