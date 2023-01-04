package com.gamma.skybase.build.server.etl.tx.hlr;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.skybase.utils.PatternPool;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

@SuppressWarnings("unchecked")
public class HLRRecordEnrichment implements IEnrichment {

    private static final Logger logger = LoggerFactory.getLogger(HLRRecordEnrichment.class);
    private static final String SOURCE_FORMAT = "yy-MM-dd HH:mm:ss";
    private static final String TARGET_FORMAT = "yyyyMMdd HH:mm:ss";
    private static final ThreadLocal<SimpleDateFormat> sourceDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat(SOURCE_FORMAT));
    private static final ThreadLocal<SimpleDateFormat> targetDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat(TARGET_FORMAT));
    private final Date currentTime = new Date();
    private LinkedHashMap<String, Object> headerRecord;

    @Override
    public MEnrichmentResponse transform(MEnrichmentReq request) {
        this.headerRecord = (LinkedHashMap<String, Object>) request.getOptionalParams();
        LinkedHashMap<String, Object> data = transform(request.getRequest());
        MEnrichmentResponse response = new MEnrichmentResponse();
        if (data != null) {
            response.setResponseCode(true);
            response.setResponse(data);
        } else {
            response.setResponseCode(false);
        }
        return response;
    }

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> record) {

        LinkedHashMap<String, Object> txRecord = new LinkedHashMap<>();
        try {
            txRecord.put("EXCHANGE_IDENTITY", headerRecord.get("exchange_identity"));
            txRecord.put("RECORD_TYPE", record.get("record_type"));
            Object servedMsisdn = record.get("msisdn");
            Object imsi = record.get("imsi");
            Object recordNumber = record.get("record_number");
            Object srvTypeKey = record.get("srv_type_key");
            Object recordType = record.get("record_type");
            if (recordType == null) recordType = record.get("record_id");

            if (servedMsisdn == null) servedMsisdn = headerRecord.get("msisdn");
            if (imsi == null) imsi = headerRecord.get("imsi");
            if (recordNumber == null) recordNumber = headerRecord.get("record_number");
            if (srvTypeKey == null) srvTypeKey = headerRecord.get("srv_type_key");

            boolean lteActivationFlag = isLteActivated(imsi);
            txRecord.put("SERVED_MSISDN", servedMsisdn);
            txRecord.put("SERVED_IMSI", imsi);
            txRecord.put("EDR_SEQ_NUM", recordNumber);
            txRecord.put("SRV_TYPE_KEY", srvTypeKey);
            txRecord.put("LTE_SUB_FLAG", lteActivationFlag ? "1": "0");

            int recordTypeId = (Integer) recordType;

            Object subsData = record.get("subs_data");
            Map<String, Object> serviceData = null;
            switch (recordTypeId) {
                case 50:  serviceData = processSubsData(subsData); break;
                case 0:  serviceData = processExtSupplementaryServiceData(subsData); break;
                case 3:  serviceData = processExtCamelData(subsData); break;
                default:
                    logger.info("record type {} not handled", recordTypeId);
                    return null;
            }
            if (serviceData!= null && !serviceData.isEmpty()) txRecord.putAll(serviceData);

            String startTimeStr = (String) headerRecord.get("record_date_time");
            Date eventStartTime = sourceDateFormat.get().parse(startTimeStr);
            txRecord.put("XDR_DATE", targetDateFormat.get().format(eventStartTime));
            Object fileName =  record.get("fileName");
            if (fileName == null) fileName = headerRecord.get("fileName");
            txRecord.put("FILE_NAME", fileName);
            txRecord.put("HLR_TYPE", getHlrType((String) fileName));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        txRecord.put("POPULATION_DATE_TIME", targetDateFormat.get().format(currentTime));
        return txRecord;
    }

    private boolean isLteActivated(Object imsiObj) {
        String imsi = (String) imsiObj;
        return imsi.startsWith("634012");
    }

    private Map<String,Object> processExtSupplementaryServiceData(Object subsData) {
        Map<String, Object> supplementaryServiceData = new LinkedHashMap<>();
        List<LinkedHashMap<String, Object>> callForwardTransformedList = new ArrayList<>();
        ArrayList<LinkedHashMap<String, Object>> supplDataset = (ArrayList<LinkedHashMap<String, Object>>) subsData;
        for (LinkedHashMap<String, Object> entry : supplDataset) {
            int servicePointer = (Integer) entry.get("service_pointer_bsg");
            String columnPrefix = null;
            switch (servicePointer) {
                case 50:
                    columnPrefix = "voice_";
                    break;
                case 53:
                    columnPrefix = "sms_";
                    break;
                case 60:
                    columnPrefix = "data_";
                    break;
            }
            if (columnPrefix != null) {
                Object ssStatusDataList = entry.get("ss_status_data_list");
                if (ssStatusDataList instanceof ArrayList) {
                    Optional<LinkedHashMap<String, Object>> optionalStatus = ((ArrayList<LinkedHashMap<String, Object>>) ssStatusDataList)
                            .stream().findFirst();
                    if (optionalStatus.isPresent()) {
                        LinkedHashMap<String, Object> status = optionalStatus.get();
                        List<String> serviceKeys = Arrays.asList("cfu", "cfb", "cfnry", "cfnrc", "baoc", "boic",
                                "boiexh", "baic", "bicro", "caw_status", "spn_status", "dcf_status");
                        for (String key : serviceKeys) {
                            String recKey = columnPrefix + key;
                            recKey = recKey.toUpperCase();
                            supplementaryServiceData.put(recKey, status.get(key));
                        }
                    }
                }

                Object callForwardData = entry.get("cf_register_data_list");
                if (callForwardData instanceof ArrayList) {

                    ArrayList<LinkedHashMap<String, Object>> callForwardList = ((ArrayList<LinkedHashMap<String, Object>>) callForwardData);
                    for (LinkedHashMap<String, Object> cfData : callForwardList) {
                        LinkedHashMap<String, Object> cfTransformedData = new LinkedHashMap<>();
                        cfTransformedData.put("SERVICE_TYPE", columnPrefix.substring(0, columnPrefix.lastIndexOf("_")));
                        cfTransformedData.put("CF_SERVICE_COND", cfData.get("servicePointer4RegisteredCFServices"));
                        cfTransformedData.put("FORWARD_NUMBER", cfData.get("forwarded2Number"));
                        cfTransformedData.put("CF_NO_REPLY_COND_TIME", cfData.get("noReplyConditionTime"));
                        callForwardTransformedList.add(cfTransformedData);
                    }
                }
            }
        }
        if (!callForwardTransformedList.isEmpty()) supplementaryServiceData.put("callForwardDataList", callForwardTransformedList);
        return supplementaryServiceData;
    }

    private Map<String, Object> processExtCamelData(Object subsData) {
        Map<String, Object> camelServiceData = new LinkedHashMap<>();
        ArrayList<LinkedHashMap<String, Object>> camelDataset = (ArrayList<LinkedHashMap<String, Object>>) subsData;
        Set<String> camelKeySet = new HashSet<>();
        for (LinkedHashMap<String, Object> camelEntity: camelDataset) {
            camelKeySet.addAll(camelEntity.keySet());
            Object octdpsPhase2Obj = camelEntity.get("octdps_phase_2");
            Object tctdpsPhase2Obj = camelEntity.get("tctdps_phase_2");
            Object camelSubsOptionsDataObj = camelEntity.get("camel_subs_options_data");
            Object extCamelDataObj = camelEntity.get("ext_camel_data");

            if (octdpsPhase2Obj != null) {
                LinkedHashMap<String, Object> octdpsPhase2 = (LinkedHashMap<String, Object>) octdpsPhase2Obj;
                camelServiceData.put("NO_OF_OCTDPS_OF_CAMEL_PH2", octdpsPhase2.get("no_of_octdps_of_camel_ph2"));
                camelServiceData.put("TDP_FOR_OCSI_OF_CAMEL_PH2", octdpsPhase2.get("tdp_for_o_csi_of_camel_ph2"));
                camelServiceData.put("SCF_ADDRESS_OCTDPS_PH2", octdpsPhase2.get("gsm_scf_address"));
            }

            if (tctdpsPhase2Obj != null) {
                LinkedHashMap<String, Object> tctdpsPhase2 = (LinkedHashMap<String, Object>) tctdpsPhase2Obj;
                camelServiceData.put("NO_OF_TCTDPS_PH2", tctdpsPhase2.get("num_tctdps_ph2"));
                camelServiceData.put("TDP_FOR_T_CSI_PH2", tctdpsPhase2.get("tdp_for_t_csi_ph2"));
                camelServiceData.put("GSM_SCF_ADDRESS_TCTDPS_PH2", tctdpsPhase2.get("gsm_scf_address"));
                camelServiceData.put("INHIBIT_T_CSI_GMSC", tctdpsPhase2.get("inhibition_of_sending_of_t_csi_towards_gmsc"));
            }

            if (camelSubsOptionsDataObj != null) {
                LinkedHashMap<String, Object> camelSubsOptionsData = (LinkedHashMap<String, Object>) camelSubsOptionsDataObj;
                camelServiceData.put("GC_SO_PH1", camelSubsOptionsData.get("gc_so_ph1"));
                camelServiceData.put("MC_SO_PH1", camelSubsOptionsData.get("mc_so_ph1"));
                camelServiceData.put("SS_LO", camelSubsOptionsData.get("ss_lo"));
                camelServiceData.put("MC_SO_PH2", camelSubsOptionsData.get("mc_so_ph2"));
                camelServiceData.put("GC_SO_PH2", camelSubsOptionsData.get("gc_so_ph2"));
                camelServiceData.put("TIF_CSI", camelSubsOptionsData.get("tif_csi"));
                camelServiceData.put("GPRSC_SO_PH3", camelSubsOptionsData.get("gprsc_so_ph3"));
                camelServiceData.put("OSMS_SO_PH3", camelSubsOptionsData.get("osms_so_ph3"));
                camelServiceData.put("TSMS_SO_PH4", camelSubsOptionsData.get("tsms_so_ph4"));
                camelServiceData.put("MMSO_PH3", camelSubsOptionsData.get("mmso_ph3"));
                camelServiceData.put("MCSO_PH3", camelSubsOptionsData.get("mcso_ph3"));
                camelServiceData.put("GCSO_PH3", camelSubsOptionsData.get("gcso_ph3"));
                camelServiceData.put("MCSO_PH4", camelSubsOptionsData.get("mcso_ph4"));
                camelServiceData.put("GCSO_PH4", camelSubsOptionsData.get("gcso_ph4"));
            }

            if (extCamelDataObj != null) {
                LinkedHashMap<String, Object> extCamelData = (LinkedHashMap<String, Object>) extCamelDataObj;
                camelServiceData.put("EXT_ORG_IN_CAP_IND", extCamelData.get("ext_org_in_cap_ind"));
                camelServiceData.put("EXT_OICK", extCamelData.get("ext_oick"));
                camelServiceData.put("EXT_TER_IN_CAP_IND", extCamelData.get("ext_ter_in_cap_ind"));
                camelServiceData.put("EXT_TICK", extCamelData.get("ext_tick"));
            }
        }

        List<String> identifiedKeys = Arrays.asList("octdps_phase_2", "tctdps_phase_2", "camel_subs_options_data", "ext_camel_data");
        for (String key :camelKeySet) {
            if (!identifiedKeys.contains(key)) {
                logger.info("new camel key {}", key);
            }
        }
        return camelServiceData;
    }



    private String getHlrType(String fileName) {
        String hlrType = "unknown";
        String regex = "(.+)\\_(.+)_(\\d{6}).*";
        String[] fields = {"fileName", "prefix1", "hlrNode", "fileDate"};
        Map<String, String> parts = PatternPool.getStatFromFileName(fileName, regex, fields);
        if (parts.containsKey("hlrNode")) {
            String hlrNode = parts.get("hlrNode");
            if (hlrNode.length() > 3) {
                String hlrNodeId = hlrNode.substring(3);
                if (StringUtils.isNumeric(hlrNodeId)) {
                    int nodeId = Integer.parseInt(hlrNodeId);
                    hlrType = nodeId % 2 == 0 ? "redundant" : "master";
                }
            }
        }

        return hlrType;
    }

    private Map<String, Object> processSubsData(Object subsData) {
        Map<String, Object> output = new LinkedHashMap<>();
        if (subsData instanceof ArrayList) {
            for (Object obj : (ArrayList<Object>) subsData) {
                LinkedHashMap<String, Object> entry = (LinkedHashMap<String, Object>) obj;
                int servicePointerSud = (Integer) entry.get("service_pointer_sud");
                int sud = (Integer) entry.get("sud");
                switch (servicePointerSud) {
                    case 2: output.put("CALL_FWD_UNCONDITIONAL_SUD", sud); break;
                    case 4: output.put("CALL_FWD_SUB_BUSY_SUD", sud); break;
                    case 5: output.put("CALL_FWD_NO_REPLY_SUD", sud); break;
                    case 6: output.put("CALL_FWD_SUB_NOT_REACHABLE_SUD", sud); break;
                    case 7: output.put("SINGLE_PERSONAL_NUM_SUD", sud); break;
                    case 8: output.put("CALL_WAITING_SUD", sud); break;
                    case 9: output.put("CALL_HOLD_SUD", sud); break;
                    case 10: output.put("MULTIPARTY_SERVICE_SUD", sud); break;
                    case 11: output.put("ADVICE_OF_CHARGE_SUD", sud); break;
                    case 15: output.put("BARRING_OUT_CALLS_SUD", sud); break;
                    case 16: output.put("BARRING_OUT_INT_CALLS_SUD", sud); break;
                    case 17: output.put("BARRING_OUT_INT_CALLS_EXCEPT_HOME_SUD", sud); break;
                    case 19: output.put("BARRING_INC_CALLS_SUD", sud); break;
                    case 20: output.put("BARRING_INC_ROAMING_OUTSIDE_HOME_SUD", sud); break;
                    case 21: output.put("SUBSCRIBER_CATEGORY_SUD", sud); break;
                    case 23: output.put("OPER_DTR_BARRING_OUT_CALLS_SUD", sud); break;
                    case 24: output.put("OPER_DTR_BARRING_INC_CALLS_SUD", sud); break;
                    case 25: output.put("OPER_DTR_BARRING_ROAMING_SUD", sud); break;
                    case 26: output.put("OPER_DTR_BARRING_OUT_PREMIUM_CALLS_INFO_SUD", sud); break;
                    case 27: output.put("OPER_DTR_BARRING_OUT_PREMIUM_CALLS_ENTER_SUD", sud); break;
                    case 28: output.put("OPER_DTR_BARRING_SUPPL_SERVICES_MGMNT_SUD", sud); break;
                    case 29: output.put("REG_HOME_PLMN_OPER_SPECIFIC_BARRING_TYPE1_SUD", sud); break;
                    case 30: output.put("REG_HOME_PLMN_OPER_SPECIFIC_BARRING_TYPE2_SUD", sud); break;
                    case 31: output.put("REG_HOME_PLMN_OPER_SPECIFIC_BARRING_TYPE3_SUD", sud); break;
                    case 32: output.put("REG_HOME_PLMN_OPER_SPECIFIC_BARRING_TYPE4_SUD", sud); break;
                    case 33: output.put("ORG_FOR_FWD_TO_NUM_ANALYSIS_SUD", sud); break;
                    case 34: output.put("SUBSCRIBER_PASSWORD_SUD", sud); break;
                    case 35: output.put("IMMEDIATE_CALL_ITEMIZATION_SUD", sud); break;
                    case 36: output.put("ORIG_INTELLIGENT_NETWORK_SUD", sud); break;
                    case 37: output.put("TERM_INTELLIGENT_NETWORK_SUD", sud); break;
                    case 38: output.put("CALLING_LINE_IDENT_PRESENT_SUD", sud); break;
                    case 39: output.put("CALLING_LINE_IDENT_RESTRICT_SUD", sud); break;
                    case 40: output.put("CONNECTED_LINE_IDENT_PRESENT_SUD", sud); break;
                    case 41: output.put("CONNECTED_LINE_IDENT_RESTRICT_SUD", sud); break;
                    case 42: output.put("SUB_OPT_CONTROL_BARRING_SERVICES_SUD", sud); break;
                    case 43: output.put("SUB_OPT_CALL_FWD_UNCONDITIONAL_SUD", sud); break;
                    case 44: output.put("SUB_OPT_CALL_FWD_SUBS_BUSY_SUD", sud); break;
                    case 45: output.put("SUB_OPT_CALL_FWD_NO_REPLY_SUD", sud); break;
                    case 46: output.put("SUB_OPT_CALL_FWD_SUBS_NOT_REACHABLE_SUD", sud); break;
                    case 47: output.put("SUB_OPT_CALLING_LINE_IDENT_PRESENT_SUD", sud); break;
                    case 48: output.put("SUB_OPT_CALLING_LINE_IDENT_RESTRICT_SUD", sud); break;
                    case 49: output.put("SUB_OPT_CONNECTED_LINE_IDENT_PRESENT_SUD", sud); break;
                    case 50: output.put("ALL_VOICE_SERVICES_SUD", sud); break;
                    case 51: output.put("TELE_SERVICE_TELEPHONY_SUD", sud); break;
                    case 53: output.put("ALL_SMS_SERVICES_SUD", sud); break;
                    case 54: output.put("TELE_SERVICE_SMS_MT_SUD", sud); break;
                    case 55: output.put("TELE_SERVICE_SMS_MO_SUD", sud); break;
                    case 56: output.put("ALL_FACSIMILE_SERVICES_SUD", sud); break;
                    case 57: output.put("TELE_SERVICE_AUTO_FACSIMILE_GROUP3_SUD", sud); break;
                    case 58: output.put("AUXILIARY_SPEECH_SERVICES_SUD", sud); break;
                    case 59: output.put("TELE_SERVICE_AUXILIARY_TELEPHONY_SUD", sud); break;
                    case 60: output.put("ALL_DATA_ASYNC_SERVICES_SUD", sud); break;
                    case 61: output.put("BEARER_SERVICE_DATA_ASYNC_300_SUD", sud); break;
                    case 62: output.put("BEARER_SERVICE_DATA_ASYNC_1200_SUD", sud); break;
                    case 63: output.put("BEARER_SERVICE_DATA_ASYNC_1200_75_SUD", sud); break;
                    case 64: output.put("BEARER_SERVICE_DATA_ASYNC_2400_SUD", sud); break;
                    case 65: output.put("BEARER_SERVICE_DATA_ASYNC_4800_SUD", sud); break;
                    case 66: output.put("BEARER_SERVICE_DATA_ASYNC_9600_SUD", sud); break;
                    case 67: output.put("ALL_DATA_SYNC_SERVICES_SUD", sud); break;
                    case 68: output.put("BEARER_SERVICE_DATA_SYNC_1200_SUD", sud); break;
                    case 69: output.put("BEARER_SERVICE_DATA_SYNC_2400_SUD", sud); break;
                    case 70: output.put("BEARER_SERVICE_DATA_SYNC_4800_SUD", sud); break;
                    case 71: output.put("BEARER_SERVICE_DATA_SYNC_9600_SUD", sud); break;
                    case 72: output.put("DEFAULT_BASIC_SERVICE_GROUP_SUD", sud); break;
                    case 80: output.put("TELE_SERVICE_ALT_SPEECH_FACSIMILE_GROUP3_SUD", sud); break;
                    case 81: output.put("CLOSED_USER_GROUP_SUD", sud); break;
                    case 82: output.put("REGIONAL_SERVICES_SUD", sud); break;
                    case 83: output.put("PREFERRED_INTER-EXCHANGE_CARRIER_ID_SUD", sud); break;
                    case 84: output.put("DEFAULT_CALL_FWD_SUD", sud); break;
                    case 85: output.put("SUB_OPT_NOTIF_DEFAULT_CALL_FWD_SUD", sud); break;
                    case 86: output.put("SUB_OPT_FWD_COND_DEFAULT_CALL_FWD_SUD", sud); break;
                    case 87: output.put("CHANNEL_ALLOCATION PRIORITY_LEVEL_SUD", sud); break;
                    case 88: output.put("ORIG_IN_CAT_KEY_SUD", sud); break;
                    case 89: output.put("TERM_IN_CAT_KEY_SUD", sud); break;
                    case 90: output.put("SUBSCRIPTION_TYPE_SUD", sud); break;
                    case 91: output.put("REDUNDANT_CONDITION_SUD", sud); break;
                    case 92: output.put("OPER_DTR_BARRING_REG_FWD_TO_NUM_SUD", sud); break;
                    case 93: output.put("OPER_DTR_BARRING_INC_INTER_ZONAL_CALLS_SUD", sud); break;
                    case 94: output.put("OPER_DTR_BARRING_OUT_INTER_ZONAL CALLS_SUD", sud); break;
                    case 95: output.put("GEN_ASYNC_BEARER_SERVICE_SUD", sud); break;
                    case 96: output.put("GEN_SYNC_BEARER_SERVICE_SUD", sud); break;
                    case 97: output.put("FALLBACK_ASYNC_BEARER SERVICE_SUD", sud); break;
                    case 98: output.put("FALLBACK_SYNC_BEARER SERVICE_SUD", sud); break;
                    case 99: output.put("MOBILITY_MGMNT_IN_TRIGGERING_SUB_DATA_SUD", sud); break;
                    case 100: output.put("ACCOUNT_CODES_SUD", sud); break;
                    case 101: output.put("ORIG_CAMEL_SUB_INFO_SUD", sud); break;
                    case 102: output.put("TERM_CAMEL_SUB_INFO_SUD", sud); break;
                    case 104: output.put("SUBS_TRACING_SUD", sud); break;
                    case 105: output.put("NETWORK_ACCESS_MODE_SUD", sud); break;
                    case 106: output.put("TRANSFER_SMS_OPTION_SUD", sud); break;
//                    case 107: output.put("ENHANCED MULTI-LEVEL PRECEDENCE AND PRE-EMPTION_SUD", sud); break;
//                    case 108: output.put("ENHANCED MULTI-LEVEL PRECEDENCE AND PRE-EMPTION DEFAULT PRIORITY LEVEL_SUD", sud); break;
//                    case 109: output.put("ENHANCED MULTI-LEVEL PRECEDENCE AND PRE-EMPTION MAXIMUM PRIORITY LEVEL_SUD", sud); break;
                    case 111: output.put("LOC_MEASUREMENT_UNIT_SUD", sud); break;
                    case 112: output.put("UNIVERSAL_LCS_PRIVACY_CLASS_SUD", sud); break;
                    case 113: output.put("CALL_SESSION_RELATED_LCS_PRIVACY_CLASS_SUD", sud); break;
                    case 114: output.put("CALL_SESSION_UNRELATED_LCS_PRIVACY_CLASS_SUD", sud); break;
                    case 115: output.put("PLMN_OPER_LCS PRIVACY CLASS_SUD", sud); break;
                    case 116: output.put("BASIC_SELF_LOC_ORIG_LCS_CLASS_SUD", sud); break;
                    case 117: output.put("AUTO_SELF_LOC_ORIG_LCS_CLASS_SUD", sud); break;
                    case 118: output.put("TRANSFER_THIRD_PARTY_ORIG_LCS_CLASS_SUD", sud); break;
                    case 119: output.put("GPRS_CAMEL_SUB_INFO_SUD", sud); break;
                    case 120: output.put("ORIG_SMS_CAMEL_SUB_INFO_SUD", sud); break;
                    case 121: output.put("SUBS_CHARGING_CHAR_SUD", sud); break;
                    case 122: output.put("TERM_SMS_CAMEL_SUB_INFO_SUD", sud); break;
                    case 123: output.put("OPER_DTR_BARRING_PACKET_ORIENTED_SERVICES_SUD", sud); break;
                    case 124: output.put("MOBILITY_MGMNT_CAMEL_SUB_INFO_SUD", sud); break;
                    case 125: output.put("VISITED_MSC_TERM_CAMEL_SUB_INFO_SUD", sud); break;
                    case 126: output.put("ROAMING_SERVICE_AREA_SUD", sud); break;
                    case 127: output.put("PREFERRED_INTER_EXCHANGE_CARRIER_ID2_SUD", sud); break;
                    case 128: output.put("PREFERRED_INTER_EXCHANGE_CARRIER_ID3_SUD", sud); break;
                    case 129: output.put("EXPLICIT_CALL_TRANSFER_SUD", sud); break;
                    case 130: output.put("OPER_DTR_BARRING_INVOC_CALL_TRANSFER_SUD", sud); break;
                    case 131: output.put("OPER_DTR_BARRING_INVOC_CALL TRANSFER WHERE BOTH CALLS ARE CHARGED TO THE SERVED SUBSCRIBER_SUD", sud); break;
                    case 132: output.put("OPER_DTR_BARRING_FURTHER INVOCATION OF CALL TRANSFER IF THERE IS ALREADY ONE ONGOING TRANSFERRED CALL FOR THE SERVED SUBSCRIBER IN THE SERVING MSC/VLR_SUD", sud); break;
                    case 133: output.put("SERVICE_TYPE_LCS_LIST_SUD", sud); break;
                    case 134: output.put("SUB_OPT_LCS_SUD", sud); break;
                    case 135: output.put("IST_ALERT_TIMER_SUD", sud); break;
                    case 136: output.put("IST_VLR_SUB_OPT_SUD", sud); break;
                    case 137: output.put("IST_CALL_SUB_OPT_SUD", sud); break;
                    case 138: output.put("IST_GMSC_SUB_OPT_SUD", sud); break;
                    case 139: output.put("DIALLED_SERVICES_CAMEL_SUB_INFO_SUD", sud); break;
                    case 140: output.put("PDP_CONTEXT_PROFILE_SUD", sud); break;
                    case 141: output.put("CAMEL_SUB_PROFILE_SUD", sud); break;
                    case 142: output.put("SPATIAL_TRIGGERS_SUD", sud); break;
                    case 143: output.put("PERSONALIZED_RBT_SUD", sud); break;
                    case 144: output.put("ANONYMOUS_CALL_REJECT_SUD", sud); break;
                    case 145: output.put("REAL_TIME_CHARGING_ALL_SUD", sud); break;
                    case 146: output.put("MUL_SUB_DATA_PROVISION_STATUS_SUD", sud); break;
                    case 147: output.put("REDUNDANCY_MECHANISM_SUD", sud); break;
                    case 148: output.put("SPAM_CONTROL_MT_SMS_ACT_STATUS_SUD", sud); break;
                    case 149: output.put("MISSED_CALL_ALERT_SUD", sud); break;
                    case 151: output.put("OPER_DTR_BARRING_CCDA_CALLS_SUD", sud); break;
                    case 152: output.put("ACCESS_RESTRICT_DATA_SUD", sud); break;
                    case 153: output.put("GPRS_ROAMING_DIST_PROFILE_SUD", sud); break;
                    case 154: output.put("ROAMING_DIST_PROFILE_SUD", sud); break;
                    case 172: output.put("MUL_REDUNDANCY_PRIMARY_HLR_ID_SUD", sud); break;
                    case 173: output.put("MUL_REDUNDANCY_MECHANISM_SUD", sud); break;
                    case 174: output.put("SUBS_HOME_PLMN_SUD", sud); break;
                    case 191: output.put("SMS_HOME_ROUTING_SCREENING1_SUD", sud); break;
                    case 192: output.put("SMS_HOME_ROUTING_SCREENING2_SUD", sud); break;
                    case 193: output.put("M2M_PROFILE_ID_SUD", sud); break;
                    case 194: output.put("OPER_DTR_BARRING_NOTIF_CAMEL_SERVICE_ENV_FLAG_SUD", sud); break;
                    case 195: output.put("CALL_FWD_NOTIF_TO_CAMEL_SERVICE_ENV_FLAG_SUD", sud); break;
                    case 196: output.put("CALL_BARRING_NOTIF_TO_CAMEL_SERVICE_ENV_FLAG_SUD", sud); break;
                    case 197: output.put("DIALLED_SERVICES_CAMEL_SUB_INFO_NOTIF_FLAG_SUD", sud); break;
                    case 198: output.put("GPRS_CAMEL_SUB_INFO_NOTIF_FLAG_SUD", sud); break;
                    case 199: output.put("MOBILITY_MGMNT_CAMEL_SUB_INFO_NOTIF_FLAG_SUD", sud); break;
                    case 200: output.put("ORIG_CAMEL_SUB_INFO_NOTIF_FLAG_SUD", sud); break;
                    case 201: output.put("ORIG_SMS_CAMEL_SUB_INFO_NOTIF_FLAG_SUD", sud); break;
                    case 202: output.put("TERM_CAMEL_SUB_INFO_NOTIF_FLAG_SUD", sud); break;
                    case 203: output.put("TERM_SMS_CAMEL_SUB_INFO_NOTIF_FLAG_SUD", sud); break;
                    case 204: output.put("VISITED_MSC_TERM_CAMEL_SUB_INFO_NOTIF_FLAG_SUD", sud); break;
                    case 205: output.put("TRANS_INFO_FLAG_TO_CAMEL_SUB_INFO_NOTIF_FLAG_SUD", sud); break;
                    case 206: output.put("DIALLED_SERVICES_CAMEL_SUB_INFO_ACT_STATE_SUD", sud); break;
                    case 207: output.put("GPRS_CAMEL_SUB_INFO_ACT_STATE_SUD", sud); break;
                    case 208: output.put("MOBILITY_MGMNT_CAMEL_SUB_INFO_ACT_STATE_SUD", sud); break;
                    case 209: output.put("ORIG_CAMEL_SUB_INFO_ACT_STATE_SUD", sud); break;
                    case 210: output.put("ORIG_SMS_CAMEL_SUB_INFO_ACT_STATE_SUD", sud); break;
                    case 211: output.put("TERM_CAMEL_SUB_INFO_ACT_STATE_SUD", sud); break;
                    case 212: output.put("TERM_SMS_CAMEL_SUB_INFO_ACT_STATE_SUD", sud); break;
                    case 213: output.put("VISITED_MSC_TERM_CAMEL_SUB_INFO_ACT_STATE_SUD", sud); break;
                    case 214: output.put("IMS_CENTRALIZED_SERVICES_IND_SUD", sud); break;
                    case 215: output.put("CALL_WAITING_NOTIF_FLAG_SUD", sud); break;
                    case 216: output.put("CALL_HOLD_NOTIF_FLAG_SUD", sud); break;
                    case 217: output.put("CONNECTED_LINE_IDENT_PRESENT_NOTIF_FLAG_SUD", sud); break;
                    case 218: output.put("CALLING_LINE_IDENT_RESTRICT_NOTIF_FLAG_SUD", sud); break;
                    case 219: output.put("EXPLICIT_CALL_TRANS_NOTIF_FLAG_SUD", sud); break;
                }
            }
        }
        return output;
    }
}
