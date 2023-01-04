//package com.gamma.skybase.build.server.etl.decoder.ccn;
//
//
//import com.gamma.components.commons.app.AppConfig;
//import com.gamma.components.commons.constants.GammaConstants;
//import com.gamma.components.structure.IDatum;
//import com.gamma.skybase.contract.decoders.*;
//import com.gamma.skybase.decoders.ccn.ChargingSystemFileProcessor;
//import com.gamma.skybase.decoders.ccn.parser.ChargingDataOutputRecord;
//import com.google.gson.Gson;
//import com.google.gson.GsonBuilder;
//import cryptix.asn1.encoding.BerDecoder;
//import cryptix.asn1.io.ASNReader;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.*;
//import java.util.*;
//
///**
// * Created by abhij on 8/15/2016
// */
//public class ChargingSystemFileExecutor extends ChargingSystemFileProcessor {
//
//    private static final Logger logger = LoggerFactory.getLogger(ChargingSystemFileExecutor.class);
//    private AppConfig appConfig = AppConfig.instance();
//    private ChargingDataOutputRecord detailOutputRecord = new ChargingDataOutputRecord();
//
//    private LinkedHashMap<String, Object> txRecord = new LinkedHashMap<>();
//
//
//    @Override
//    @SuppressWarnings("Duplicates")
//    public void parseFile(String fileName) throws Exception {
//        boolean jsonOutputRequired = dataSource.isRawJsonEnabled();
//        String fn = null;
//        Gson gson = null;
//        if (jsonOutputRequired) {
//            fn = new File(fileName).getName();
//            gson = new GsonBuilder().setPrettyPrinting().create();
//        }
//
//        IEnrichment enrichment = null;
//        try {
//            Class exec = Class.forName(dataSource.getTxExecClass());
//            enrichment = (IEnrichment) exec.newInstance();
//        } catch (ClassNotFoundException |InstantiationException |IllegalAccessException e) {
//            logger.error(e.getMessage(), e);
//        }
//
//        IContextParameterHandler contextParameterHandler = null;
//        try{
//            String contextParamExecClass = appConfig.getProperty("app.datasource.ccn.context-param-exec-class");
//            if(contextParamExecClass != null) {
//                Class exec = Class.forName(contextParamExecClass);
//                contextParameterHandler = (IContextParameterHandler) exec.newInstance();
//            }
//        } catch (ClassNotFoundException |InstantiationException |IllegalAccessException e) {
//            logger.error(e.getMessage(), e);
//        }
//
//
//        try (FileInputStream fis = new FileInputStream(fileName)) {
//            ASNReader ber;
//            try {
//                ber = BerDecoder.class.newInstance();
//            } catch (InstantiationException | IllegalAccessException e) {
//                logger.error(e.getMessage(), e);
//                throw new IOException();
//            }
//            ber.open(fis);
//            long recCount = 0;
//            List<LinkedHashMap<String, Object>> records = new LinkedList<>();
//            while (!ber.isEOF()) {
//                LinkedHashMap<String, Object> rec = new LinkedHashMap<>();
//                try {
//                    detailOutputRecord.decode(ber, rec);
//                    decodeContextParameters(contextParameterHandler, rec);
//                    if (jsonOutputRequired) records.add(rec);
//                } catch (EOFException e) {//java.io.EOFException, possible all records are good
//                    break;
//                } catch (Exception e) {
//                    metadata.comments = "Parsing issues";
//                    logger.info("Error " + metadata.toString());
//                    logger.error(e.getMessage(), e);
//                    break;
//                }
//                processRecord(rec, enrichment);
//                recCount++;
//                metadata.totalNoOfRecords = recCount;
//            }
//            ber.close();
//
//            if (jsonOutputRequired) {
//                FileWriter writer = new FileWriter("out"+ GammaConstants.PATH_SEP+fn + ".json");
//                writer.write(gson.toJson(records));
//                writer.flush();
//            }
//        }
//    }
//
//    @SuppressWarnings("Duplicates")
//    private void decodeContextParameters(IContextParameterHandler contextParameterHandler, LinkedHashMap<String, Object> rec) {
//
//        if(contextParameterHandler != null){
//            if(rec.get("onlineCreditControlRecord") != null) {
//                ArrayList<LinkedHashMap<String, Object>> olccEvents = (ArrayList<LinkedHashMap<String, Object>>) rec.get("onlineCreditControlRecord");
//                if(olccEvents != null) {
//                    for (LinkedHashMap<String, Object> r : olccEvents) {
//                        ArrayList<LinkedHashMap<String, Object>> creditControlRecord = (ArrayList<LinkedHashMap<String, Object>>) r.get("creditControlRecord");
//                        if(creditControlRecord != null) {
//                            for (LinkedHashMap<String, Object> ccr : creditControlRecord) {
//                                Object chargingContextSpecific = ccr.get("chargingContextSpecific");
//                                if (chargingContextSpecific != null) {
////                                    List<LinkedHashMap<String, Object>> ctxParameters = (List<LinkedHashMap<String, Object>>) chargingContextSpecific;
//                                    List<List<LinkedHashMap<String, Object>>> ctxParameters = (List<List<LinkedHashMap<String, Object>>>) chargingContextSpecific;
//                                    contextParameterHandler.decodeValues(ctxParameters);
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//        }
//    }
//
//    @SuppressWarnings("Duplicates")
//    private void processRecord(LinkedHashMap<String, Object> rec, IEnrichment enrichment) {
//        Set<Map.Entry<String, Object>> entries = rec.entrySet();
//        for (Map.Entry<String, Object> eventType : entries) {
//            switch (eventType.getKey()) {
//                case "diameterCreditControlRecord":
//                    getDCCEventTree("diameterCreditControlRecord", rec, "");
//                    if (txRecord.get("teleServiceCode").toString().equals("4")
//                            || txRecord.get("teleServiceCode").toString().equals("0")) break;
//                    try {
//                        handleEvents("DiameterCreditControlRecord", txRecord);
//                    } catch (Exception e) {
//                        logger.error(e.getMessage(), e);
//                    }
//                    txRecord.clear();
//                    break;
//
//                case "onlineCreditControlRecord":
////                    System.out.println("\n----\n" + toJson(rec).toString(2) + "\n----\n");
//                    ArrayList<LinkedHashMap<String, Object>> olccEvents = (ArrayList<LinkedHashMap<String, Object>>) rec.get("onlineCreditControlRecord");
//                    for (LinkedHashMap<String, Object> r : olccEvents) {
//                        try {
//                            if(enrichment != null) {
//                                MEnrichmentReq request = new MEnrichmentReq();
//                                request.setRequest(r);
//                                MEnrichmentResponse response = enrichment.transform(request);
//                                LinkedHashMap<String,Object> data = response.getResponse();
//                                if(response.isResponseCode()) {
//                                    String _EVENT_TYPE_ = data.get("_EVENT_TYPE_").toString();
//                                    data.put("fileName", this.metadata.decompFileURL.substring(this.metadata.decompFileURL.lastIndexOf(File.separator) + 1, this.metadata.decompFileURL.length()));
//                                    // demo event consideration SCF_PRE, SCF_POST
//                                    if("SCF_PRE".equalsIgnoreCase(_EVENT_TYPE_) || "SCF_POST".equalsIgnoreCase(_EVENT_TYPE_)){
//                                        handleEvents(_EVENT_TYPE_, data);
//                                    }
//                                    metadata.noOfParsedRecord++;
////                                handleEvents("ALL", data);
//                                }
//                            }
//
//                        } catch (Exception e) {
//                            logger.error(e.getMessage(), e);
//                        }
//                    }
//                    break;
//                case "fBCRatingRecord":
//                case "rTCCreditControlRecord":
//                case "sCFPDPRecord":
//                case "sCFSMSPSMORecord":
//                case "sCFSMSCSMORecord":
//                default:
//                    logger.info(eventType.getKey() + " record type not handled.");
//            }
//        }
//    }
//
//
//    private void getDCCEventTree(String key, LinkedHashMap<String, Object> rec, String p) {
//        Set<Map.Entry<String, Object>> entrySet = rec.entrySet();
//        for (Map.Entry<String, Object> entry : entrySet) {
//
//            Object triggerTime = null;
//            if (entry.getKey().equalsIgnoreCase("triggerTime"))
//                triggerTime = entry.getValue();
//
//            Object subscriberID = "";
//            if (entry.getKey().equalsIgnoreCase("subscriberID"))
//                subscriberID = entry.getValue();
//
//            Object value = entry.getValue();
//            if (value instanceof ArrayList) {
//                for (Object o : (ArrayList) value) {
//                    getDCCEventTree(entry.getKey(), (LinkedHashMap<String, Object>) o, p + '\t');
//                }
//                if ("accumulatorValueInfo".equalsIgnoreCase(entry.getKey())) {
//                    for (LinkedHashMap<String, Object> acc : (List<LinkedHashMap<String, Object>>) value) {
//                        acc.put("subscriberID", subscriberID);
//                        acc.put("triggerTime", triggerTime);
//                        try {
////                            handleEvents("DCC_Accumulator", acc);
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                    }
//                } else if ("dedicatedAccountInfo".equalsIgnoreCase(entry.getKey())) {
//                    for (LinkedHashMap<String, Object> da : (List<LinkedHashMap<String, Object>>) value) {
//                        da.put("subscriberID", subscriberID);
//                        da.put("triggerTime", triggerTime);
//                        try {
////                            handleEvents("DCC_DedicatedAccount", da);
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                    }
//                }
//            } else {
//                txRecord.put(entry.getKey(), entry.getValue());
//            }
//        }
//    }
//
//    @Override
//    public void flush() {
//        try {
//            super.flush();
//        } catch (SkybaseDeserializerException e) {
//            logger.error(e.getMessage(), e);
//        }
//    }
//
//    @Override
//    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {
//
//    }
//}
