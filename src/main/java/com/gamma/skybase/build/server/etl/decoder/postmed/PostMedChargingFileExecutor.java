package com.gamma.skybase.build.server.etl.decoder.postmed;

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

import static com.gamma.components.commons.constants.GammaConstants.PATH_SEP;

/**
 * Created by abhi on 12/01/22
 */
@SuppressWarnings("Duplicates")
public class PostMedChargingFileExecutor extends PostMedChargingFileProcessor{
    private static final Logger logger = LoggerFactory.getLogger(PostMedChargingFileExecutor.class);
    private List<LinkedHashMap<String, Object>> jsonRecords;

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

        String [] headers = null;
        String adapterName = dataSource.getAdapterName().toLowerCase();
        switch (adapterName) {
            case "eric_pm_gy_pre":
                headers = new String[]{"LOCALSEQUENCENUMBER", "NODEID", "TELESERVICECODE", "SUBSCRIBERID", "CALLINGPARTYNUMBER", "CALLEDPARTYNUMBER",
                        "TRIGGERTIME", "DATAVOLUME", "ACCOUNTVALUEBEFORE", "ACCOUNTVALUEAFTER", "FINALCHARGE", "SESSIONID", "USERNAME", "ORIGINHOST",
                        "SERVICECLASS", "ORIGINATINGLOCATIONINFO", "FAMILYANDFRIENDSINDICATOR", "ACCUMULATORID", "DAFIRSTID", "DAFIRSTVALUEBEFORE",
                        "DAFIRSTVALUEAFTER", "SERVICEPROVIDERID", "EXTINT1", "EXTINT2", "EXTINT3", "EXTINT4", "EXTTEXT", "SMSDELIVERYSTATUS",
                        "NUMBEROFEVENTS", "DA_ID_1", "DA_BEFORE_1", "DA_AFTER_1", "DA_CHANGE_1", "DA_ID_2", "DA_BEFORE_2", "DA_AFTER_2", "DA_CHANGE_2",
                        "DA_ID_3", "DA_BEFORE_3", "DA_AFTER_3", "DA_CHANGE_3", "DA_ID_4", "DA_BEFORE_4", "DA_AFTER_4", "DA_CHANGE_4", "DA_ID_5",
                        "DA_BEFORE_5", "DA_AFTER_5", "DA_CHANGE_5", "SERVICEOFFERINGS", "UA_ID_1", "UA_BEFORE_1", "UA_AFTER_1", "UA_CHANGE_1",
                        "UA_ID_2", "UA_BEFORE_2", "UA_AFTER_2", "UA_CHANGE_2", "UA_ID_3", "UA_BEFORE_3", "UA_AFTER_3", "UA_CHANGE_3", "UA_ID_4",
                        "UA_BEFORE_4", "UA_AFTER_4", "UA_CHANGE_4", "UA_ID_5", "UA_BEFORE_5", "UA_AFTER_5", "UA_CHANGE_5", "OFFERID", "CHARGETIME",
                        "CGI", "FLEX_FLD3", "FLEX_FLD4", "FLEX_FLD5", "UC_ID_1", "UC_BEFORE_1", "UC_AFTER_1", "UC_CHANGE_1", "UC_ID_2", "UC_BEFORE_2",
                        "UC_AFTER_2", "UC_CHANGE_2", "UC_ID_3", "UC_BEFORE_3", "UC_AFTER_3", "UC_CHANGE_3", "UC_ID_4", "UC_BEFORE_4", "UC_AFTER_4",
                        "UC_CHANGE_4", "UC_ID_5", "UC_BEFORE_5", "UC_AFTER_5", "UC_CHANGE_5", "EXTRA1", "UNKNOWN1", "UNKNOWN2", "UNKNOWN3"};

                /* hotfix for May re-processing */
                if (fileName.contains("-2022050") || fileName.contains("-2022051") || fileName.contains("-2022052") || fileName.contains("-2022053")){
                    headers = new String[]{"LOCALSEQUENCENUMBER", "NODEID", "TELESERVICECODE", "SUBSCRIBERID", "CALLINGPARTYNUMBER", "CALLEDPARTYNUMBER",
                            "TRIGGERTIME", "DATAVOLUME", "ACCOUNTVALUEBEFORE", "ACCOUNTVALUEAFTER", "FINALCHARGE", "SESSIONID", "USERNAME", "ORIGINHOST",
                            "SERVICECLASS", "ORIGINATINGLOCATIONINFO", "FAMILYANDFRIENDSINDICATOR", "ACCUMULATORID", "DAFIRSTID", "DAFIRSTVALUEBEFORE",
                            "DAFIRSTVALUEAFTER", "SERVICEPROVIDERID", "EXTINT1", "EXTINT2", "EXTINT3", "EXTINT4", "EXTTEXT", "SMSDELIVERYSTATUS",
                            "NUMBEROFEVENTS", "DA_ID_1", "DA_BEFORE_1", "DA_AFTER_1", "DA_CHANGE_1", "DA_ID_2", "DA_BEFORE_2", "DA_AFTER_2", "DA_CHANGE_2",
                            "DA_ID_3", "DA_BEFORE_3", "DA_AFTER_3", "DA_CHANGE_3", "DA_ID_4", "DA_BEFORE_4", "DA_AFTER_4", "DA_CHANGE_4", "DA_ID_5",
                            "DA_BEFORE_5", "DA_AFTER_5", "DA_CHANGE_5", "SERVICEOFFERINGS", "UA_ID_1", "UA_BEFORE_1", "UA_AFTER_1", "UA_CHANGE_1",
                            "UA_ID_2", "UA_BEFORE_2", "UA_AFTER_2", "UA_CHANGE_2", "UA_ID_3", "UA_BEFORE_3", "UA_AFTER_3", "UA_CHANGE_3", "UA_ID_4",
                            "UA_BEFORE_4", "UA_AFTER_4", "UA_CHANGE_4", "UA_ID_5", "UA_BEFORE_5", "UA_AFTER_5", "UA_CHANGE_5", "OFFERID", "CHARGETIME",
                            "CGI", "FLEX_FLD3", "FLEX_FLD4", "FLEX_FLD5", "UC_ID_1", "UC_BEFORE_1", "UC_AFTER_1", "UC_CHANGE_1", "UC_ID_2", "UC_BEFORE_2",
                            "UC_AFTER_2", "UC_CHANGE_2", "UC_ID_3", "UC_BEFORE_3", "UC_AFTER_3", "UC_CHANGE_3", "UC_ID_4", "UC_BEFORE_4", "UC_AFTER_4",
                            "UC_CHANGE_4", "UC_ID_5", "UC_BEFORE_5", "UC_AFTER_5", "UC_CHANGE_5", "EXTRA1"};
                }
                break;
            case "eric_pm_gy_post":
                headers = new String[]{"LOCALSEQUENCENUMBER", "NODEID", "TELESERVICECODE", "SUBSCRIBERID", "CALLINGPARTYNUMBER", "CALLEDPARTYNUMBER",
                        "TRIGGERTIME", "DATAVOLUME", "ACCOUNTVALUEBEFORE", "ACCOUNTVALUEAFTER", "FINALCHARGE", "SESSIONID", "USERNAME", "ORIGINHOST",
                        "SERVICECLASS", "ORIGINATINGLOCATIONINFO", "FAMILYANDFRIENDSINDICATOR", "ACCUMULATORID", "DAFIRSTID", "DAFIRSTVALUEBEFORE",
                        "DAFIRSTVALUEAFTER", "SERVICEPROVIDERID", "EXTINT1", "EXTINT2", "EXTINT3", "EXTINT4", "EXTTEXT", "SMSDELIVERYSTATUS",
                        "NUMBEROFEVENTS", "DA_ID_1", "DA_BEFORE_1", "DA_AFTER_1", "DA_CHANGE_1", "DA_ID_2", "DA_BEFORE_2", "DA_AFTER_2", "DA_CHANGE_2",
                        "DA_ID_3", "DA_BEFORE_3", "DA_AFTER_3", "DA_CHANGE_3", "DA_ID_4", "DA_BEFORE_4", "DA_AFTER_4", "DA_CHANGE_4", "DA_ID_5",
                        "DA_BEFORE_5", "DA_AFTER_5", "DA_CHANGE_5", "DACHANGE21", "DACHANGE22", "DACHANGE23", "DACHANGE24", "DACHANGE25", "DACHANGE26",
                        "DACHANGE27", "DACHANGE28", "DACHANGE29", "DACHANGE30", "DACHANGE31", "DACHANGE32", "DACHANGE33", "DACHANGE34", "DACHANGE35",
                        "UC_ID_1", "UC_BEFORE_1", "UC_AFTER_1", "UC_CHANGE_1", "UC_ID_2", "UC_BEFORE_2", "UC_AFTER_2", "UC_CHANGE_2", "UC_ID_3",
                        "UC_BEFORE_3", "UC_AFTER_3", "UC_CHANGE_3", "UC_ID_4", "UC_BEFORE_4", "UC_AFTER_4", "UC_CHANGE_4", "UC_ID_5", "UC_BEFORE_5",
                        "UC_AFTER_5", "UC_CHANGE_5", "SERVICEOFFERINGS", "UA_ID_1", "UA_BEFORE_1", "UA_AFTER_1", "UA_CHANGE_1", "UA_ID_2", "UA_BEFORE_2",
                        "UA_AFTER_2", "UA_CHANGE_2", "UA_ID_3", "UA_BEFORE_3", "UA_AFTER_3", "UA_CHANGE_3", "UA_ID_4", "UA_BEFORE_4", "UA_AFTER_4", "UA_CHANGE_4",
                        "UA_ID_5", "UA_BEFORE_5", "UA_AFTER_5", "UA_CHANGE_5", "UACHANGE21", "UACHANGE22", "UACHANGE23", "UACHANGE24", "UACHANGE25", "UACHANGE26",
                        "UACHANGE27", "UACHANGE28", "UACHANGE29", "UACHANGE30", "UACHANGE31", "UACHANGE32", "UACHANGE33", "UACHANGE34", "UACHANGE35", "UACHANGE36",
                        "UACHANGE37", "UACHANGE38", "UACHANGE39", "UACHANGE40", "UACHANGE41", "UACHANGE42", "UACHANGE43", "UACHANGE44", "UACHANGE45", "UACHANGE46",
                        "UACHANGE47", "UACHANGE48", "UACHANGE49", "UACHANGE50", "UACHANGE51", "UACHANGE52", "UACHANGE53", "UACHANGE54", "UACHANGE55", "OFFERID",
                        "CHARGETIME", "CGI", "FLEX_FLD3", "FLEX_FLD4", "FLEX_FLD5", "EXTRA1"};
                break;
            case "eric_pm_scf_pre":
                headers = new String[]{"TIMEFOREVENT", "INCOMPLETECALLDATAINDICATOR", "TRAFFICCASEID", "NETWORKID", "SERVICECLASS", "ACCOUNTVALUEBEFORE",
                        "ACCOUNTVALUEAFTER", "FINALCHARGEOFCALL", "DURATION", "SDPCDRTYPE", "TELESERVICECODE", "TIMEZONEFORSTARTOFCHARGING", "FAFINDICATOR",
                        "REDIRECTIONINFORMATION", "CALLSETUPRESULTCODE", "TRIGGERTIME", "SUBSCRIBERNUMBER", "ORIGLOCATIONINFO", "CALLINGNUMBER",
                        "CALLEDPARTYNET", "CALLEDNUMBER", "REDIRECTINGNUMBER", "ACCOUNTNUMBER", "TERMLOCATIONINFO", "SELECTEDCOMMUNITYINDICATOR",
                        "COMMUNITYCHARGEDSUB1", "COMMUNITYCHARGEDSUB2", "COMMUNITYCHARGEDSUB3", "COMMUNITYNONCHARGEDSUB1", "COMMUNITYNONCHARGEDSUB2",
                        "COMMUNITYNONCHARGEDSUB3", "DEDICATEDACCOUNT1", "DEDICATEDACCOUNT2", "DEDICATEDACCOUNT3", "DEDICATEDACCOUNT4", "DEDICATEDACCOUNT5",
                        "DEDICATEDACCOUNT6", "DEDICATEDACCOUNT7", "DEDICATEDACCOUNT8", "DEDICATEDACCOUNT9", "DEDICATEDACCOUNT10", "DEDICATEDACCOUNT11",
                        "DEDICATEDACCOUNT12", "DEDICATEDACCOUNT13", "DEDICATEDACCOUNT14", "DEDICATEDACCOUNT15", "DEDICATEDACCOUNT16", "DEDICATEDACCOUNT17",
                        "DEDICATEDACCOUNT18", "DEDICATEDACCOUNT19", "DEDICATEDACCOUNT20", "DEDICATEDACCOUNT21", "DEDICATEDACCOUNT22", "DEDICATEDACCOUNT23",
                        "DEDICATEDACCOUNT24", "DEDICATEDACCOUNT25", "DEDICATEDACCOUNT26", "DEDICATEDACCOUNT27", "DEDICATEDACCOUNT28", "DEDICATEDACCOUNT29",
                        "DEDICATEDACCOUNT30", "DEDICATEDACCOUNT31", "DEDICATEDACCOUNT32", "DEDICATEDACCOUNT33", "DEDICATEDACCOUNT34", "DEDICATEDACCOUNT35",
                        "DEDICATEDACCOUNT36", "DEDICATEDACCOUNT37", "DEDICATEDACCOUNT38", "DEDICATEDACCOUNT39", "DEDICATEDACCOUNT40", "DEDICATEDACCOUNT41",
                        "DEDICATEDACCOUNT42", "DEDICATEDACCOUNT43", "DEDICATEDACCOUNT44", "DEDICATEDACCOUNT45", "DEDICATEDACCOUNT46", "DEDICATEDACCOUNT47",
                        "DEDICATEDACCOUNT48", "DEDICATEDACCOUNT49", "DEDICATEDACCOUNT50", "DEDICATEDACCOUNT51", "DEDICATEDACCOUNT52", "DEDICATEDACCOUNT53",
                        "DEDICATEDACCOUNT54", "DEDICATEDACCOUNT55", "USAGEACCUMULATOR1", "USAGEACCUMULATOR2", "USAGEACCUMULATOR3", "USAGEACCUMULATOR4",
                        "USAGEACCUMULATOR5", "USAGEACCUMULATOR6", "USAGEACCUMULATOR7", "USAGEACCUMULATOR8", "USAGEACCUMULATOR9", "USAGEACCUMULATOR10",
                        "USAGEACCUMULATOR11", "USAGEACCUMULATOR12", "EXTRA1", "UNKNOWN1", "UNKNOWN2", "UNKNOWN3"};

                /* hotfix for May re-processing */
                if (fileName.contains("-2022050") || fileName.contains("-2022051") || fileName.contains("-2022052") || fileName.contains("-2022053")){
                    headers = new String[]{"TIMEFOREVENT", "INCOMPLETECALLDATAINDICATOR", "TRAFFICCASEID", "NETWORKID", "SERVICECLASS", "ACCOUNTVALUEBEFORE",
                            "ACCOUNTVALUEAFTER", "FINALCHARGEOFCALL", "DURATION", "SDPCDRTYPE", "TELESERVICECODE", "TIMEZONEFORSTARTOFCHARGING", "FAFINDICATOR",
                            "REDIRECTIONINFORMATION", "CALLSETUPRESULTCODE", "TRIGGERTIME", "SUBSCRIBERNUMBER", "ORIGLOCATIONINFO", "CALLINGNUMBER",
                            "CALLEDPARTYNET", "CALLEDNUMBER", "REDIRECTINGNUMBER", "ACCOUNTNUMBER", "TERMLOCATIONINFO", "SELECTEDCOMMUNITYINDICATOR",
                            "COMMUNITYCHARGEDSUB1", "COMMUNITYCHARGEDSUB2", "COMMUNITYCHARGEDSUB3", "COMMUNITYNONCHARGEDSUB1", "COMMUNITYNONCHARGEDSUB2",
                            "COMMUNITYNONCHARGEDSUB3", "DEDICATEDACCOUNT1", "DEDICATEDACCOUNT2", "DEDICATEDACCOUNT3", "DEDICATEDACCOUNT4", "DEDICATEDACCOUNT5",
                            "DEDICATEDACCOUNT6", "DEDICATEDACCOUNT7", "DEDICATEDACCOUNT8", "DEDICATEDACCOUNT9", "DEDICATEDACCOUNT10", "DEDICATEDACCOUNT11",
                            "DEDICATEDACCOUNT12", "DEDICATEDACCOUNT13", "DEDICATEDACCOUNT14", "DEDICATEDACCOUNT15", "DEDICATEDACCOUNT16", "DEDICATEDACCOUNT17",
                            "DEDICATEDACCOUNT18", "DEDICATEDACCOUNT19", "DEDICATEDACCOUNT20", "DEDICATEDACCOUNT21", "DEDICATEDACCOUNT22", "DEDICATEDACCOUNT23",
                            "DEDICATEDACCOUNT24", "DEDICATEDACCOUNT25", "DEDICATEDACCOUNT26", "DEDICATEDACCOUNT27", "DEDICATEDACCOUNT28", "DEDICATEDACCOUNT29",
                            "DEDICATEDACCOUNT30", "DEDICATEDACCOUNT31", "DEDICATEDACCOUNT32", "DEDICATEDACCOUNT33", "DEDICATEDACCOUNT34", "DEDICATEDACCOUNT35",
                            "DEDICATEDACCOUNT36", "DEDICATEDACCOUNT37", "DEDICATEDACCOUNT38", "DEDICATEDACCOUNT39", "DEDICATEDACCOUNT40", "DEDICATEDACCOUNT41",
                            "DEDICATEDACCOUNT42", "DEDICATEDACCOUNT43", "DEDICATEDACCOUNT44", "DEDICATEDACCOUNT45", "DEDICATEDACCOUNT46", "DEDICATEDACCOUNT47",
                            "DEDICATEDACCOUNT48", "DEDICATEDACCOUNT49", "DEDICATEDACCOUNT50", "DEDICATEDACCOUNT51", "DEDICATEDACCOUNT52", "DEDICATEDACCOUNT53",
                            "DEDICATEDACCOUNT54", "DEDICATEDACCOUNT55", "USAGEACCUMULATOR1", "USAGEACCUMULATOR2", "USAGEACCUMULATOR3", "USAGEACCUMULATOR4",
                            "USAGEACCUMULATOR5", "USAGEACCUMULATOR6", "USAGEACCUMULATOR7", "USAGEACCUMULATOR8", "USAGEACCUMULATOR9", "USAGEACCUMULATOR10",
                            "USAGEACCUMULATOR11", "USAGEACCUMULATOR12", "EXTRA1"};
                }
                break;
            case "eric_pm_scf_post":
                headers = new String[]{"TIMEFOREVENT", "INCOMPLETECALLDATAINDICATOR", "TRAFFICCASEID", "NETWORKID", "SERVICECLASS", "ACCOUNTVALUEBEFORE", "ACCOUNTVALUEAFTER",
                        "FINALCHARGEOFCALL", "DURATION", "SDPCDRTYPE", "TELESERVICECODE", "TIMEZONEFORSTARTOFCHARGING", "FAFINDICATOR", "REDIRECTIONINFORMATION", "CALLSETUPRESULTCODE",
                        "TRIGGERTIME", "SUBSCRIBERNUMBER", "ORIGLOCATIONINFO", "CALLINGNUMBER", "CALLEDPARTYNET", "CALLEDNUMBER", "REDIRECTINGNUMBER", "ACCOUNTNUMBER", "TERMLOCATIONINFO",
                        "SELECTEDCOMMUNITYINDICATOR", "COMMUNITYCHARGEDSUB1", "COMMUNITYCHARGEDSUB2", "COMMUNITYCHARGEDSUB3", "COMMUNITYNONCHARGEDSUB1", "COMMUNITYNONCHARGEDSUB2",
                        "COMMUNITYNONCHARGEDSUB3", "DEDICATEDACCOUNT1", "DEDICATEDACCOUNT2", "DEDICATEDACCOUNT3", "DEDICATEDACCOUNT4", "DEDICATEDACCOUNT5", "DEDICATEDACCOUNT6",
                        "DEDICATEDACCOUNT7", "DEDICATEDACCOUNT8", "DEDICATEDACCOUNT9", "DEDICATEDACCOUNT10", "DEDICATEDACCOUNT11", "DEDICATEDACCOUNT12", "DEDICATEDACCOUNT13",
                        "DEDICATEDACCOUNT14", "DEDICATEDACCOUNT15", "DEDICATEDACCOUNT16", "DEDICATEDACCOUNT17", "DEDICATEDACCOUNT18", "DEDICATEDACCOUNT19", "DEDICATEDACCOUNT20",
                        "DEDICATEDACCOUNT21", "DEDICATEDACCOUNT22", "DEDICATEDACCOUNT23", "DEDICATEDACCOUNT24", "DEDICATEDACCOUNT25", "DEDICATEDACCOUNT26", "DEDICATEDACCOUNT27",
                        "DEDICATEDACCOUNT28", "DEDICATEDACCOUNT29", "DEDICATEDACCOUNT30", "DEDICATEDACCOUNT31", "DEDICATEDACCOUNT32", "DEDICATEDACCOUNT33", "DEDICATEDACCOUNT34",
                        "DEDICATEDACCOUNT35", "DEDICATEDACCOUNT36", "DEDICATEDACCOUNT37", "DEDICATEDACCOUNT38", "DEDICATEDACCOUNT39", "DEDICATEDACCOUNT40", "DEDICATEDACCOUNT41",
                        "DEDICATEDACCOUNT42", "DEDICATEDACCOUNT43", "DEDICATEDACCOUNT44", "DEDICATEDACCOUNT45", "DEDICATEDACCOUNT46", "DEDICATEDACCOUNT47", "DEDICATEDACCOUNT48",
                        "DEDICATEDACCOUNT49", "DEDICATEDACCOUNT50", "DEDICATEDACCOUNT51", "DEDICATEDACCOUNT52", "DEDICATEDACCOUNT53", "DEDICATEDACCOUNT54", "DEDICATEDACCOUNT55",
                        "USAGEACCUMULATOR1", "USAGEACCUMULATOR2", "USAGEACCUMULATOR3", "USAGEACCUMULATOR4", "USAGEACCUMULATOR5", "USAGEACCUMULATOR6", "USAGEACCUMULATOR7", "USAGEACCUMULATOR8",
                        "USAGEACCUMULATOR9", "USAGEACCUMULATOR10", "USAGEACCUMULATOR11", "USAGEACCUMULATOR12", "USAGEACCUMULATOR13", "USAGEACCUMULATOR14", "USAGEACCUMULATOR15",
                        "USAGEACCUMULATOR16", "USAGEACCUMULATOR17", "USAGEACCUMULATOR18", "USAGEACCUMULATOR19", "USAGEACCUMULATOR20", "USAGEACCUMULATOR21", "USAGEACCUMULATOR22",
                        "USAGEACCUMULATOR23", "USAGEACCUMULATOR24", "USAGEACCUMULATOR25", "USAGEACCUMULATOR26", "USAGEACCUMULATOR27", "USAGEACCUMULATOR28", "USAGEACCUMULATOR29",
                        "USAGEACCUMULATOR30", "USAGEACCUMULATOR31", "USAGEACCUMULATOR32", "USAGEACCUMULATOR33", "USAGEACCUMULATOR34", "USAGEACCUMULATOR35", "USAGEACCUMULATOR36",
                        "USAGEACCUMULATOR37", "USAGEACCUMULATOR38", "USAGEACCUMULATOR39", "USAGEACCUMULATOR40", "USAGEACCUMULATOR41", "USAGEACCUMULATOR42", "USAGEACCUMULATOR43",
                        "USAGEACCUMULATOR44", "USAGEACCUMULATOR45", "USAGEACCUMULATOR46", "USAGEACCUMULATOR47", "USAGEACCUMULATOR48", "USAGEACCUMULATOR49", "USAGEACCUMULATOR50",
                        "USAGEACCUMULATOR51", "USAGEACCUMULATOR52", "USAGEACCUMULATOR53", "USAGEACCUMULATOR54", "USAGEACCUMULATOR55", "SERVICEOFFERINGS", "OTHERCALLEDPARTYNET",
                        "OTHERCALLEDNUMBER", "OFFERID1", "OFFERID2", "OFFERID3", "CALL_REFERENCE", "EXTRA1"};
                break;
            case "eric_pm_scapv2_pre":
                headers = new String[]{"LOCALSEQUENCENUMBER", "NODEID", "TELESERVICECODE", "SUBSCRIBERID", "CALLINGPARTYNUMBER", "CALLEDPARTYNUMBER",
                        "TRIGGERTIME", "DATAVOLUME", "ACCOUNTVALUEBEFORE", "ACCOUNTVALUEAFTER", "FINALCHARGE", "SESSIONID", "USERNAME", "ORIGINHOST",
                        "SERVICECLASS", "ORIGINATINGLOCATIONINFO", "FAMILYANDFRIENDSINDICATOR", "ACCUMULATORID", "DAFIRSTID", "DAFIRSTVALUEBEFORE",
                        "DAFIRSTVALUEAFTER", "SERVICEPROVIDERID", "EXTINT1", "EXTINT2", "EXTINT3", "EXTINT4", "EXTTEXT", "SMSDELIVERYSTATUS", "NUMBEROFEVENTS",
                        "DACHANGE1", "DACHANGE2", "DACHANGE3", "DACHANGE4", "DACHANGE5", "DACHANGE6", "DACHANGE7", "DACHANGE8", "DACHANGE9", "DACHANGE10",
                        "DACHANGE11", "DACHANGE12", "DACHANGE13", "DACHANGE14", "DACHANGE15", "DACHANGE16", "DACHANGE17", "DACHANGE18", "DACHANGE19",
                        "DACHANGE20", "DACHANGE21", "DACHANGE22", "DACHANGE23", "DACHANGE24", "DACHANGE25", "DACHANGE26", "DACHANGE27", "DACHANGE28",
                        "DACHANGE29", "DACHANGE30", "DACHANGE31", "DACHANGE32", "DACHANGE33", "DACHANGE34", "DACHANGE35", "DACHANGE36", "DACHANGE37",
                        "DACHANGE38", "DACHANGE39", "DACHANGE40", "SERVICEOFFERINGS", "UC1", "UC2", "UC3", "UC4", "UC5", "UC6", "UC7", "UC8", "UC9",
                        "UC10", "UC11", "UC12", "UC13", "UC14", "UC15", "UC16", "UC17", "UC18", "UC19", "UC20", "EXTRA1"};
                break;
            case "eric_pm_scapv2_post":
                headers = new String[]{"LOCALSEQUENCENUMBER", "NODEID", "TELESERVICECODE", "SUBSCRIBERID", "CALLINGPARTYNUMBER", "CALLEDPARTYNUMBER",
                        "TRIGGERTIME", "DATAVOLUME", "ACCOUNTVALUEBEFORE", "ACCOUNTVALUEAFTER", "FINALCHARGE", "SESSIONID", "USERNAME", "ORIGINHOST",
                        "SERVICECLASS", "ORIGINATINGLOCATIONINFO", "FAMILYANDFRIENDSINDICATOR", "ACCUMULATORID", "DAFIRSTID", "DAFIRSTVALUEBEFORE",
                        "DAFIRSTVALUEAFTER", "SERVICEPROVIDERID", "EXTINT1", "EXTINT2", "EXTINT3", "EXTINT4", "EXTTEXT", "SMSDELIVERYSTATUS", "NUMBEROFEVENTS",
                        "DA_ID_1", "DA_BEFORE_1", "DA_AFTER_1", "DA_CHANGE_1", "DA_ID_2", "DA_BEFORE_2", "DA_AFTER_2", "DA_CHANGE_2", "DA_ID_3", "DA_BEFORE_3",
                        "DA_AFTER_3", "DA_CHANGE_3", "DA_ID_4", "DA_BEFORE_4", "DA_AFTER_4", "DA_CHANGE_4", "DA_ID_5", "DA_BEFORE_5", "DA_AFTER_5",
                        "DA_CHANGE_5", "DACHANGE21", "DACHANGE22", "DACHANGE23", "DACHANGE24", "DACHANGE25", "DACHANGE26", "DACHANGE27", "DACHANGE28",
                        "DACHANGE29", "DACHANGE30", "DACHANGE31", "DACHANGE32", "DACHANGE33", "DACHANGE34", "DACHANGE35", "DACHANGE36", "DACHANGE37",
                        "DACHANGE38", "DACHANGE39", "DACHANGE40", "DACHANGE41", "DACHANGE42", "DACHANGE43", "DACHANGE44", "DACHANGE45", "DACHANGE46",
                        "DACHANGE47", "DACHANGE48", "DACHANGE49", "DACHANGE50", "DACHANGE51", "DACHANGE52", "DACHANGE53", "DACHANGE54", "DACHANGE55",
                        "SERVICEOFFERINGS", "UA_ID_1", "UA_BEFORE_1", "UA_AFTER_1", "UA_CHANGE_1", "UA_ID_2", "UA_BEFORE_2", "UA_AFTER_2", "UA_CHANGE_2",
                        "UA_ID_3", "UA_BEFORE_3", "UA_AFTER_3", "UA_CHANGE_3", "UA_ID_4", "UA_BEFORE_4", "UA_AFTER_4", "UA_CHANGE_4", "UA_ID_5",
                        "UA_BEFORE_5", "UA_AFTER_5", "UA_CHANGE_5", "UA_ID_6", "UA_BEFORE_6", "UA_AFTER_6", "UA_CHANGE_6", "UACHANGE25", "UACHANGE26",
                        "UACHANGE27", "UACHANGE28", "UACHANGE29", "UACHANGE30", "UACHANGE31", "UACHANGE32", "UACHANGE33", "UACHANGE34", "UACHANGE35",
                        "UACHANGE36", "UACHANGE37", "UACHANGE38", "UACHANGE39", "UACHANGE40", "UACHANGE41", "UACHANGE42", "UACHANGE43", "UACHANGE44",
                        "UACHANGE45", "UACHANGE46", "UACHANGE47", "UACHANGE48", "UACHANGE49", "UACHANGE50", "UACHANGE51", "UACHANGE52", "UACHANGE53",
                        "UACHANGE54", "UACHANGE55", "EXTRA1"};
                break;
        }
        if (headers != null) {
            DelimitedFileDecoder decoder = new DelimitedFileDecoder(fileName, '|', headers, 0);
            try {
                logger.info("Adapter {} decoding File {} ", dataSource.getAdapterName(), new File(fileName).getName());
                long recCount = 0;
                while (decoder.hasNext()) {
                    try {
                        LinkedHashMap<String, Object> record = decoder.next();
                        record.put("fileName", metadata.decompFileName);
                        if (jsonOutputRequired) jsonRecords.add(record);
                        processRecord(record, enrichment);

                    }  catch (Exception e) {
                        metadata.comments = "Parsing issues";
                        metadata.noOfBadRecord++;
                        logger.error(dataSource.getAdapterName() + " -> " + e.getMessage(), e);
                    }
                    recCount++;
                    metadata.totalNoOfRecords = recCount;
                }
            }  catch (Exception e) {
                decoder.close();
            }
        }

        if (jsonOutputRequired) {
            FileWriter writer = new FileWriter("out" + PATH_SEP + dataSource.getAdapterName() + PATH_SEP + fn + ".json");
            writer.write(gson.toJson(jsonRecords));
            writer.flush();
        }
    }

    private void processRecord(LinkedHashMap<String, Object> record, IEnrichment enrichment) {
        try {
            if (enrichment != null) {
                String datasourceName = dataSource.getAdapterName();
                MEnrichmentReq request = new MEnrichmentReq();
                Map<String, Object> optionalParams = new LinkedHashMap<>();
                optionalParams.put("fileName", record.remove("fileName"));
                optionalParams.put("source", this.dataSource.getAdapterName());
                request.setOptionalParams(optionalParams);
                request.setRequest(record);
                MEnrichmentResponse response = enrichment.transform(request);
                LinkedHashMap<String, Object> data = response.getResponse();
                if (datasourceName.equalsIgnoreCase("eric_pm_gy_pre")) {
                    handleEvents("ERIC_PM_GY_PRE", data);
                } else if (datasourceName.equalsIgnoreCase("eric_pm_gy_post")) {
                    handleEvents("ERIC_PM_GY_POST", data);
                } else if (datasourceName.equalsIgnoreCase("eric_pm_scf_pre")) {
                    handleEvents("ERIC_PM_SCF_PRE", data);
                } else if (datasourceName.equalsIgnoreCase("eric_pm_scf_post")) {
                    handleEvents("ERIC_PM_SCF_POST", data);
                } else if (datasourceName.equalsIgnoreCase("eric_pm_scapv2_pre")) {
                    handleEvents("ERIC_PM_SCAP_PRE", data);
                } else if (datasourceName.equalsIgnoreCase("eric_pm_scapv2_post")) {
                    handleEvents("ERIC_PM_SCAP_POST", data);
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }


    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }
}
