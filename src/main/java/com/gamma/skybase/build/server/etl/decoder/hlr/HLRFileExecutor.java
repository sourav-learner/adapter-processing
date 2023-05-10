package com.gamma.skybase.build.server.etl.decoder.hlr;

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
public class HLRFileExecutor extends HLRFileProcessor {
    private static final Logger logger = LoggerFactory.getLogger(HLRFileExecutor.class);
    private List<LinkedHashMap<String, Object>> jsonRecords;

      String[] headers = {"HLR_INDEX" , "IMSI" , "MSISDN" , "GPRS_SUBSCRIBED" , "APN" , "PDP_ADD" , "NAM" , "ODBIC" , "ODBOC" , "ODBPLMN1" , "ODBPLMN2" , "ODBPLMN3" , "ODBPLMN4" , "ODBROAM" , "ODBDECT" , "ODBMECT" , "ODBINFO" , "ODBECT" , "ODBPOS" , "ODBPOSTYPE" , "ODBRCF" , "ODBENTER" , "ODBSS" , "LOCK_SABLCKIN" , "LOCK_SABLCKOUT" , "LOCK_MSCLCKIN" , "LOCK_MSCLCKOUT" , "LOCK_SGSNLCK" , "UTRANNOTALLOWED" , "GERANNOTALLOWED" , "CARD_TYPE" , "CATEGORY" , "TS10" , "TS11" , "TS12" , "TS20" , "TS21" , "TS22" , "TS60" , "TS61" , "TS62" , "BS20" , "BS21" , "BS22" , "BS23" , "BS24" , "BS25" , "BS26" , "BS2G" , "BS30" , "BS31" , "BS32" , "BS33" , "BS34" , "BS3G" , "BS40" , "BS41" , "BS42" , "BS43" , "BS44" , "BS45" , "BS46" , "BS4G" , "BS50" , "BS51" , "BS52" , "BS53" , "BS5G" , "BS61" , "BS62" , "BS81" , "BS82" , "CF_CFU" , "CFU_FTN" , "CF_CFB" , "CFB_FTN" , "CF_CFNRY" , "CFNRY_FTN" , "CF_CFNRC" , "CFNRC_FTN" , "CF_CFD" , "CFD_FTN" , "CB_BAOC" , "CB_BOIC" , "CB_BAIC" , "CLIP" , "CLIP_OVERRIDE" , "CLIR" , "CLIROPTION" , "COLP" , "COLR" , "CW" , "ECT" , "HOLD" , "MPTY" , "OCSITPL" , "TCSITPL" , "UCSITPL" , "SMSCSITPL" , "MTSMSCSITPL" , "GPRSCSITPL" , "SSCSITPL" , "VBS" , "VGCS" , "VLR_NUMBER" , "MSC_NUMBER" , "SGSN_NUMBER" , "SGSN_ADDRESS" , "MSCRESTRICTFLAG" , "SGSNRESTRICTFLAG" , "VLRTPL" , "SGSNTPL" , "RRRTPL" , "CUG" , "PLMNSPECIFICTS_1" , "PLMNSPECIFICTS_2" , "PLMNSPECIFICTS_3" , "PLMNSPECIFICTS_4" , "PLMNSPECIFICTS_5" , "PLMNSPECIFICTS_6" , "PLMNSPECIFICTS_7" , "PLMNSPECIFICTS_8" , "PLMNSPECIFICTS_9" , "PLMNSPECIFICTS_A" , "PLMNSPECIFICTS_B" , "PLMNSPECIFICTS_C" , "PLMNSPECIFICTS_D" , "PLMNSPECIFICTS_E" , "PLMNSPECIFICTS_F" , "ALLPLMN_SPECIFICSS" , "PLMNSPECIFICSS_1" , "PLMNSPECIFICSS_2" , "PLMNSPECIFICSS_3" , "PLMNSPECIFICSS_4" , "PLMNSPECIFICSS_5" , "PLMNSPECIFICSS_6" , "PLMNSPECIFICSS_7" , "PLMNSPECIFICSS_8" , "PLMNSPECIFICSS_9" , "PLMNSPECIFICSS_A" , "PLMNSPECIFICSS_B" , "PLMNSPECIFICSS_C" , "PLMNSPECIFICSS_D" , "PLMNSPECIFICSS_E" , "PLMNSPECIFICSS_F" , "VLRINHPLMN" , "VLRINTERNATIONAL" , "SGSNINHPLMN" , "SGSNINTERNATIONAL" , "SUBRES" , "ACR_STATUS" , "SMSROUTETPL_ID" , "UPLCSLCK" , "UPLPSLCK" , "UPL_TIME" , "PURGE_TIME_ATMSC" , "GPRSUPL_TIME" , "PURGE_TIME_ATSGSN" , "SMDP" , "LCS" , "ARD" , "VTCSI" , "DCSI" , "TIFCSI" , "MCSI" , "CHARGE_GLOBA" , "OPT_GPRSTPL_ID" , "OKSC" , "GANNOTALLOWED" , "IHSPAENOTALLOWED" , "EUTRANNOTALLOWED" , "N3GPPNOTALLOWED" , "EPSLOCK" , "NON3GPPLOCK" , "STANDBY_IMSI" , "EXPIRY_DATE" , "STD_CHARGE_GLOBAL" , "USERCATEGORY" , "SMSCF" , "SMS_OFATPL_ID" , "SMS_FTN" , "SMSCF_ssStaus" , "SMSCF_ftnInHPLMNC" , "SMSCF_ftnInVPLMNC" , "SMSCF_ftnEnter" , "SMSCF_ftnInfo" , "SMSCF_ftnVMS" , "MCI" , "CNAP" , "CCBS_B" , "CCBS_A" , "MAH" , "AOCC" , "AOCI" , "UUS_1" , "UUS_2" , "UUS_3" , "MC_NBR_SB" , "MC_NBR_USER" , "MAX_PRIORITY" , "DEF_PRIORITY" , "NLR" , "VVDN" , "COLPOC" , "CONTROL" , "WPA" , "PW" , "CARP" , "VGCSR" , "VBSR" , "ACR" , "IST_ALERT_TIMER" , "IST_ALERT_RESPONSE" , "NAEA" , "MIMSI_TYPE" , "MIMSI_SMS" , "MIMSI_VOBB" , "CURRENT_PSI_INDEX" , "EOCSI_SK" , "ETCSI_SK" , "ACC_LENGTH" , "STYPE" , "RBTINDEX" , "PRE_VOICENUMBER" , "SEC_VOICENUMBER" , "EXEXROUTECATEGORY" , "NIR_INDEX" , "MMEHOST" , "SGSNHOST" , "MMETIMESTAMP" , "STNSR" , "APNOI" , "TGPPAMBRMAXUL" , "TGPPAMBRMAXDL" , "RATFREQSELPRI" , "PLMNTPLID" , "DIAMNODETPL_ID" , "FMInit" , "FMSupervisor" , "FMRemote" , "EPS_QOSTPL_ID" , "PROFILE_ID" , "MCSISTATE" , "OCSISTATE" , "TCSISTATE" , "TS63" , "ROUTECATEGORY" , "GPRS_OPTION_GPRS_ACCESS" , "CAMEL_ACTIVE_O_COLL_INFO_DP" , "STATUS_SUBS_DEACT" , "CORE_INAP" , "SCP" , "SMS_SERV_KEY" , "BOIC" , "BIRO" , "BORO" , "BAOC" , "BAIC" , "OPTGPRS_CONTEXTID" , "APNTPLID" , "QOSTPLID" , "VPLMN" , "OPTGPRS_PDPADD" , "OPT_CHARGE_DETAILS" , "APN_TYPE" , "DEFAULT_FLAG" , "WILDCARD_FLAG" , "PDP_TYPE" , "STD_CHARGE_OPT" , "PDP_ADDIPV4" , "PURGEDONMME" , "PURGEDONSGSN" , "SERSELECTION" , "APNDYN_CONTEXTID" , "MIPHAHOST" , "MIPHAREALM" , "MIPHAADDRIPV4" , "MIPHAADDRIPV6" , "WILDCARDFLAG" , "VNI" , "APNDYN_TIME_STAMP" , "APNDYN_LOCATION_ID" , "ACTIVETIMER" , "EUTRANEDRXCYCLE" , "NBIOTEDRXCYCLE" , "ETICK" , "NON3GPP_TIME_STAMP_UPL" , "ARBT_INDEX" , "CS_UTRAN" , "CS_GERAN" , "CS_GAN" , "CS_I_HSPA" , "CS_E_UTRAN" , "PS_UTRAN" , "PS_GERAN" , "PS_GAN" , "PS_I_HSPA" , "PS_E_UTRAN" , "IMEI_SVN" , "APNOITPLID" , "PS_CONTEXT_ID" , "PS_APNQOSTPLID" , "PS_PDP_ADD" , "PS_ADD_INDICATOR" , "PS_PDP_ADDIPV4" , "EPS_CONTEXT_ID" , "EPS_APNQOSTPLID" , "EPS_PDP_ADD" , "EPS_ADD_INDICATOR" , "EPS_PDP_ADDIPV4" , "EPS_DEFAULT_FLAG" , "EOICK" , "NAEACICTYPE" , "CAPL" , "ICI" , "PSODBROAM" , "EPSODBPOS" , "CS_ROAMPERTPL_ID" , "PS_ROAMPERTPL_ID" , "SPN_OFATPL_ID" , "STE" , "GMLC_ADD" , "UCSITPLID" , "ETCSICONVER_TPLID" , "O_IM_CSI" , "VT_IM_CSI" , "MAXEXTBRUPL" , "MAXEXTBRDWL" , "LONG_FTN_SUPPORTED" , "RAUTAUTIMER" , "EUTRANNBIOTNOTALLOWED" , "EPS_DEFAULTNONIP_FLAG" , "UEUSAGETYPE" , "NGRANNOTALLOWED" , "AMBRNRUL" , "AMBRNRDL" , "NSA5G" , "SUB_CREATE_TIME" , "RRR_TPL_ID_PS" , "APNLIST_APN_TYPE" , "APNLIST_CONTEXT_ID" , "APNLIST_APN" , "EPSLCKOPTGPRSFLAG"};

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
            long recCount = 1;

            decoder = new DelimitedFileDecoder(fileName, ',', headers, 0);
            while (decoder.hasNext()) {
                try {
                    LinkedHashMap<String, Object> record = decoder.next();
                    if (jsonOutputRequired) jsonRecords.add(record);
                    record.put("_SEQUENCE_NUMBER",recCount);
                    record.put("fileName", metadata.decompFileName);
                    processRecord(record, enrichment);
                } catch (Exception e) {
                    e.printStackTrace();
                    metadata.comments = "Parsing issues";
                    metadata.noOfBadRecord++;
                    logger.error(dataSource.getAdapterName() + " -> " , e.getMessage(), e);
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
                    handleEvents("HLR_FCT", data);
            }
        }
    }

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }
}