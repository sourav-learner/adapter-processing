package com.gamma.skybase.build.server.etl.decoder.cudb;

import com.gamma.components.structure.IDatum;
import com.gamma.skybase.contract.decoders.IEnrichment;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class CUDBFileExecutor extends CUDBFileProcessor {
    private static final Logger logger = LoggerFactory.getLogger(CUDBFileProcessor.class);
    static Map<String, String[]> headers = new LinkedHashMap<>();

    static String[] IMS_CUDBService = {"createTimestamp", "modifyTimestamp", "structuralObjectClass", "assocId", "entryDS", "serv", "ou", "objectClass", "nodeId", "dc", "mscId"};
    static String[] IMS_ImsImpu_sip = {"createTimestamp", "modifyTimestamp", "structuralObjectClass", "CDC", "entryDS", "serv", "ou", "objectClass", "ImsSessBarrInd", "IMPU", "assocId", "ImsIsDefault", "ImsMaxNumberOfContacts", "ImsAssocImpi", "ImsIrs", "nodeId", "dc"};
    static String[] IMS_ImsShDynInf_sip = {"createTimestamp", "modifyTimestamp", "ImsShData", "structuralObjectClass", "CDC", "entryDS", "serv", "ou", "objectClass", "IMPU", "ImsShDynInfId", "assocId", "nodeId", "dc"};
    static String[] IMS_ImsZxDynInf_sip = {"createTimestamp", "modifyTimestamp", "ImsZxDynInfId", "structuralObjectClass", "assocId", "entryDS", "serv", "ou", "objectClass", "IMPU", "nodeId", "dc"};
    static String[] IMS_ImsImpu_tel = {"createTimestamp", "modifyTimestamp", "structuralObjectClass", "CDC", "entryDS", "serv", "ou", "objectClass", "ImsSessBarrInd", "IMPU", "assocId", "ImsIsDefault", "ImsMaxNumberOfContacts", "ImsAssocImpi", "ImsIrs", "nodeId", "dc"};
    static String[] IMS_ImsShDynInf_tel = {"createTimestamp", "modifyTimestamp", "ImsShData", "structuralObjectClass", "CDC", "entryDS", "serv", "ou", "objectClass", "IMPU", "ImsShDynInfId", "assocId", "nodeId", "dc"};
    static String[] IMS_ImsZxDynInf_tel = {"createTimestamp", "modifyTimestamp", "ImsZxDynInfId", "structuralObjectClass", "assocId", "entryDS", "serv", "ou", "objectClass", "IMPU", "nodeId", "dc"};
    static String[] IMS_ImsServProf = {"createTimestamp", "modifyTimestamp", "structuralObjectClass", "assocId", "entryDS", "serv", "ou", "ImsServProfId", "objectClass", "ImsConfServProf", "nodeId", "ImsMaxSimultSess", "dc"};
    static String[] IMS_ImsSubs = {"createTimestamp", "modifyTimestamp", "ImsCharProfId", "ImsChargingId", "structuralObjectClass", "entryDS", "serv", "ou", "objectClass", "ImsSubsId", "assocId", "ImsPrivacyInd", "ImsIsPsi", "ImsAssocImpi", "nodeId", "dc"};
    static String[] Auth_CUDBService = {"createTimestamp", "modifyTimestamp", "IMSI", "structuralObjectClass", "entryDS", "serv", "ou", "objectClass", "nodeId", "mscId", "dc"};
    static String[] Auth_AU1 = {"createTimestamp", "modifyTimestamp", "IMSI", "CDC", "SQNIMS", "SQNPS", "BNS", "FSETIND", "AMFVALUE", "mscId", "AKAALGIND", "SQNCS", "VNUMBER", "A3A8IND", "GAPSIGN", "A4IND", "EKI", "structuralObjectClass", "entryDS", "serv", "ou", "objectClass", "SQN", "GAP", "KIND", "AKATYPE", "nodeId", "dc", "MIGRATIONEXPDATE", "MIGRATIONSTEP", "SEQHE"};
    static String[] CSPS_CP1 = {"createTimestamp", "modifyTimestamp", "IMSI", "MSISDN", "CFUTS60ST", "CFNRY", "CDC", "RVLRI", "CFUBS30ST", "SCHAR", "CFNRYTS60ST", "BS3G", "CFNRYBS30ST", "PWD", "SOCB", "HOLD", "UNIV", "entryDS", "serv", "SOCFNRC", "objectClass", "SOCFNRY", "STYPE", "SOCLIR", "SOCLIP", "CAT", "CAW", "nodeId", "CFB", "SOCFU", "CFBTS60ST",  "CAWTS60ST", "RSGSNI", "BS26", "OFA", "MPTY", "DBSG", "mscId", "PDPCP", "CLIP", "CFNRCTS10ST", "CFBBS30ST", "CFU", "CAWBS30ST", "SOCFB", "GSMUEFEAT", "SOCOLP", "CFNRCBS20ST", "TS22", "TS21", "TS62", "OBR", "SUBSPDPCPVERS", "structuralObjectClass", "PWDC", "CFNRYTS10ST", "ou", "OICK", "CSLOC", "CFBTS10ST", "CAWTS10ST", "CFNRC", "CFBBS20ST", "SOSDCF", "CAWBS20ST", "CFUBS20ST", "TS11", "CFNRCTS60ST", "NAM", "CFUTS10ST", "SODCF", "CFNRYBS20ST", "CFNRCBS30ST", "dc", "SCLOCSTATE", "CSIVLRSUPP", "SZONELOCSTATE", "DCF", "MSCARF", "GSMLCSSUPP", "PSLOC", "VLRADD", "DCFTS10ST", "MIGRATIONEXPDATE", "UNKNLOCDATEPS", "GSMDUALNUMSUP", "IMEISV", "DCFTS10TIME", "THLRN", "MIGRATIONSTEP", "BSGDCFREG", "AUTHINFO", "BSGDCFACTOP", "GSMMSRNMSCN", "DCFTS10ZCREL", "GSMMAPVERS", "DCFTS10CCREL", "PRBT", "DCFTS10FNUM", "BOICTS60ST", "BICROBS20ST", "BICROTS60ST", "BOICBS20ST", "BOICTS10ST", "BAICTS20ST", "BOIEXHTS10ST", "BOIC", "BAOCTS20ST", "BOIEXHBS20ST", "BAOC", "UNKNLOCDATECS", "BAOCBS20ST", "BOIEXHTS60ST", "BAICTS10ST", "BOICTS20ST", "BAIC", "BICROTS20ST", "BAOCTS60ST", "BAICTS60ST", "GPRSOBP", "BOIEXH", "BAICBS20ST", "BOIEXHTS20ST", "BICRO", "BAOCTS10ST", "BICROTS10ST", "GPRSRELSUPP", "SGSNNUM", "OBO", "PURGEDATEPS", "GPRSODBNSUPP", "VLRISTSUPP", "PURGEDATECS", "GPRSODBMAPPING", "CSISGSNSUPP", "DCFBS20ST", "DCFTS60ST", "DCFBS20CCREL", "DCFTS60FNUM", "DCFTS60ZCREL", "DCFBS20TIME", "DCFBS20ZCREL", "DCFTS60TIME", "DCFTS60CCREL", "DCFBS20FNUM", "DEMLPP", "CAMP", "MEMLPP", "EMLPP", "SUBSCSPVERS", "CSP", "CFUTS10FNUM", "CFUTS10CCREL", "CFUTS10ZCREL", "OBI", "LMSID", "CFNRCTS10CCREL", "CFNRCTS10FNUM", "CFNRCTS10ZCREL", "CFNRYTS10ZCREL", "CFNRYTS10CCREL", "CFNRYTS10TIME", "CFNRYTS10FNUM", "CFBTS10FNUM", "CFBTS10CCREL", "CFBTS10ZCREL", "DCFBS30ZCREL", "DCFBS30TIME", "DCFBS30FNUM", "DCFBS30ST", "DCFBS30CCREL", "RSA", "SUBSRSAVERS", "BAOCBS30ST", "BAICBS30ST", "BICROBS30ST", "BOICBS30ST", "OBSSM", "CFUBS30FNUM", "CFUBS30ZCREL", "CFUBS30CCREL", "PDPTYPE1", "EQOSID1", "APNID1", "APNVERS1", "EQOSIDV1", "VPAA1", "CFNRCBS30CCREL", "CFNRCBS30ZCREL", "CFNRCBS30FNUM", "CFUTS60CCREL", "CFUBS20CCREL", "CFUTS60ZCREL", "CFUBS20ZCREL", "CFUTS60FNUM", "CFUBS20FNUM", "CFBBS30FNUM", "CFBBS30CCREL", "CFBBS30ZCREL", "CFBBS20FNUM", "CFBTS60CCREL", "CFBTS60ZCREL", "CFBBS20CCREL", "CFBTS60FNUM", "CFBBS20ZCREL", "CAMEL4CSIVLR", "OIN", "CUGIC1", "CUGINDEX1", "PCUGTS10", "CUGBSG1", "CUGICNI1", "GSMCUGSUPPORT", "CUGTS10ACCESS", "CFNRYBS30ZCREL", "CFNRYBS30CCREL", "CFNRYBS30TIME", "CFNRYBS30FNUM", "CFNRCBS20CCREL", "CFNRYTS60CCREL", "CFNRCBS20FNUM", "CFNRCTS60CCREL", "CFNRYBS20ZCREL", "CFNRYTS60FNUM", "CFNRCTS60FNUM", "CFNRYBS20CCREL", "CFNRYTS60ZCREL", "CFNRCBS20ZCREL", "CFNRYBS20FNUM", "CFNRYBS20TIME", "CFNRYTS60TIME", "CFNRCTS60ZCREL", "PDPADD1", "EQOSIDV7", "EQOSID7", "PDPTYPE7", "PDPADD7", "APNID7", "VPAA7", "APNVERS7", "AOC", "TICK", "BOIEXHBS30ST", "GSMBS2GNSUPP", "GSMBS3GNSUPP", "COLP", "GSMVGMLCADD", "EQOSID10", "APNVERS10", "APNID10", "PDPTYPE10", "VPAA10", "EQOSIDV10", "EQOSID6", "PDPADD6", "PDPTYPE6", "TS61", "APNID6", "APNVERS6", "EQOSIDV6", "VPAA6", "PDPADD10", "PDPTYPE4", "EQOSID4", "APNID4", "APNVERS4", "EQOSIDV4", "VPAA4", "PDPADD4", "PDPTYPE5", "EQOSID5", "APNID5", "APNVERS5", "EQOSIDV5", "VPAA5", "EQOSID11", "PDPTYPE11", "APNVERS11", "APNID11", "VPAA11", "EQOSIDV11", "PDPTYPE3", "EQOSID3", "APNID3", "APNVERS3", "EQOSIDV3", "VPAA3", "EQOSID14", "EQOSID8", "PDPTYPE8", "PDPADD8", "PDPTYPE14", "APNID8", "APNVERS8", "EQOSIDV8", "APNVERS14", "APNID14", "VPAA14", "EQOSIDV14", "VPAA8", "PDPTYPE2", "EQOSID2", "APNID2", "APNVERS2", "EQOSIDV2", "VPAA2", "CLIR", "BS25", "BS24", "BS23", "BS22", "EQOSID9", "PDPTYPE9", "APNID9", "APNVERS9", "EQOSIDV9", "VPAA9", "EQOSID12", "PDPTYPE12", "PDPADD5", "APNVERS12", "APNID12", "VPAA12", "EQOSIDV12"};
    static String[] CSPS_alias_camel = {"createTimestamp", "modifyTimestamp", "IMSI", "CSP", "structuralObjectClass", "entryDS", "serv", "ei", "ou", "objectClass", "nodeId", "mscId", "dc"};
    static String[] CSPS_alias_gprs = {"createTimestamp", "modifyTimestamp", "IMSI", "structuralObjectClass", "entryDS", "serv", "ei", "ou", "objectClass", "nodeId", "mscId", "PDPCP", "dc"};
    static String[] AAA_CUDBService = {"createTimestamp", "modifyTimestamp", "structuralObjectClass", "entryDS", "serv", "ou", "objectClass", "nodeId", "mscId", "dc"};
    static String[] Identities_CUDBService = {"createTimestamp", "modifyTimestamp", "MSISDN", "msisdnMask", "structuralObjectClass", "CDC", "entryDS", "serv", "impiMask", "ou", "objectClass", "mscId", "nodeId", "IMPI", "dc"};

    private BufferedReader fileReader;
    long lineNo, blockNo;
    boolean hasNext = false;
    private String carryForwardLine = null;

    public static void main(String[] args) throws Exception {
        CUDBFileExecutor decoder = new CUDBFileExecutor();
        decoder.parseFile("C:\\data\\cudb\\Cudb_dump_02032022.ldif");
//        CUDBFileExecutor decoder = new CUDBFileExecutor("C:\\data\\cudb\\Cudb_dump_02032022.ldif");

        HashMap<String, BufferedWriter> writers = new HashMap<>();

        while (decoder.hasNext()) {
//            LinkedHashMap<String, Object> record = decoder.next();
            List<List<String>> block = decoder.getBlock();

            List<Map<String, String>> subRecords = block.stream()
                    .map(CUDBFileExecutor::createRecord)
                    .collect(Collectors.toList());

            AtomicReference<String> event = new AtomicReference<>();
            Map<String, Map<String, Object>> record = subRecords.stream()
                    .collect(Collectors.toMap(p -> {
                        String serv = p.get("serv");
                        String structuralObjectClass = p.get("structuralObjectClass");
                        if (structuralObjectClass != null)
                            event.set(serv + '_' + structuralObjectClass);
                        if (serv != null) {
                            if ("IMS".equalsIgnoreCase(serv)) {
                                String impu = p.get("IMPU");
                                if (impu != null) {
                                    String proto = impu.substring(0, 3);
                                    event.set(event.get() + '_' + proto);
                                }
                            } else {
                                String ei = p.get("ei");
                                if (ei != null)
                                    event.set(event.get() + '_' + ei);
                            }
                        }
                        String id = p.get("IMSI");
                        if (id == null) id = p.get("assocId");
                        if (id == null) id = p.get("mscId");
//                        System.out.println(id + " -> " + event.get());

//                        Map<String, String> h = headerNames.computeIfAbsent(event.get(), k -> new LinkedHashMap<>());
//                        h.putAll(p);

                        return event.get();
                    }, p -> {
                        String[] h = headers.get(event.get());
                        LinkedHashMap<String, Object> r = new LinkedHashMap<>();
                        for (String f : h) r.put(f, "");
                        r.putAll(p);
                        return r;
                    }));

            record.forEach((recordType, value) -> {
                BufferedWriter bw = writers.get(recordType);
                try {
                    if (bw == null) {
                        String[] h = headers.get(recordType);
                        String fileName = "C:\\data\\cudb\\output\\Cudb_dump_02032022_" + recordType + ".csv";
                        bw = Files.newBufferedWriter(Paths.get(fileName), StandardCharsets.UTF_8);
                        bw.write(String.join(", ", h) + "\n");
                        writers.put(recordType, bw);
                    }
                    String values = value.values().stream()
                            .map(Object::toString)
                            .collect(Collectors.joining(", "));
                    bw.write(values + "\n");
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            });

        }
        writers.values().forEach(e -> {
            try {
                e.close();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    public void parseFile(String fileName) throws Exception {
        headers.put("IMS_CUDBService", IMS_CUDBService);
        headers.put("IMS_ImsImpu_sip", IMS_ImsImpu_sip);
        headers.put("IMS_ImsShDynInf_sip", IMS_ImsShDynInf_sip);
        headers.put("IMS_ImsZxDynInf_sip", IMS_ImsZxDynInf_sip);
        headers.put("IMS_ImsImpu_tel", IMS_ImsImpu_tel);
        headers.put("IMS_ImsShDynInf_tel", IMS_ImsShDynInf_tel);
        headers.put("IMS_ImsZxDynInf_tel", IMS_ImsZxDynInf_tel);
        headers.put("IMS_ImsServProf", IMS_ImsServProf);
        headers.put("IMS_ImsSubs", IMS_ImsSubs);
        headers.put("Auth_CUDBService", Auth_CUDBService);
        headers.put("CSPS_CP1", CSPS_CP1);
        headers.put("Auth_AU1", Auth_AU1);
        headers.put("CSPS_alias_camel", CSPS_alias_camel);
        headers.put("CSPS_alias_gprs", CSPS_alias_gprs);
        headers.put("AAA_CUDBService", AAA_CUDBService);
        headers.put("Identities_CUDBService", Identities_CUDBService);

        System.out.println("\n\n File name " + fileName);


//        boolean outputRequired = true; //dataSource.isRawJsonEnabled();
//            FileWriter writer = new FileWriter("out" + GammaConstants.PATH_SEP + jsonFileName + ".json");
//        String jsonFileName = null;
//        if (outputRequired) {
//            jsonFileName = new File(fileName).getName();
//            outFileWriter = Files.newBufferedWriter(Paths.get(jsonFileName), StandardCharsets.UTF_8);
//        }

        IEnrichment enrichment = null;
        try {
            Class<?> exec = Class.forName(dataSource.getTxExecClass());
            enrichment = (IEnrichment) exec.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            logger.error(e.getMessage(), e);
        }

//        CUDBFileExecutor decoder = new CUDBFileExecutor();
        fileReader = Files.newBufferedReader(Paths.get(fileName), StandardCharsets.UTF_8);
        try {
            logger.info("Adapter {} decoding File {} ", dataSource.getAdapterName(), new File(fileName).getName());
            long recCount = 0;
            while (hasNext()) {
                try {
                    LinkedHashMap<String, Object> record = next();
                    record.put("fileName", this.metadata.decompFileURL.substring(this.metadata.decompFileURL.lastIndexOf(File.separator) + 1));
                    record.put("fileDate", this.metadata.srcFileDate);
                    processRecord(record, enrichment);
                    metadata.noOfParsedRecord++;
                } catch (Exception e) {
//                    System.out.println("\n\n\nhello\n\n");
//                    e.printStackTrace();
                    metadata.comments = "Parsing issues";
                    metadata.noOfBadRecord++;
                    logger.error(dataSource.getAdapterName() + " -> " + e.getMessage(), e);
                }
                recCount++;
                metadata.totalNoOfRecords = recCount;

                //        if (outputRequired) {
                //            writer.write(gson.toJson(jsonRecords));
                //            writer.flush();
                //        }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        fileReader.close();
    }

    public boolean hasNext() throws IOException {
//        System.out.println(fileReader != null);
        if (!hasNext)
            for (String line; (line = fileReader.readLine()) != null; ) {
                lineNo++;
                line = line.trim();
                if (line.equals("")) continue;
                if (line.startsWith("dn:")) {
                    carryForwardLine = line;
                    hasNext = true;
                    return true;
                }
            }
        return hasNext;
    }

    public LinkedHashMap<String, Object> next() throws IOException {
        List<List<String>> block = getBlock();

        List<Map<String, String>> subRecords = block.stream()
                .map(CUDBFileExecutor::createRecord)
                .collect(Collectors.toList());

        AtomicReference<String> event = new AtomicReference<>();
        Map<String, LinkedHashMap<String, Object>> record = subRecords.stream()
                .collect(Collectors.toMap(p -> {
                    String serv = p.get("serv");
                    String structuralObjectClass = p.get("structuralObjectClass");
                    if (structuralObjectClass != null)
                        event.set(serv + '_' + structuralObjectClass);
                    if (serv != null) {
                        if ("IMS".equalsIgnoreCase(serv)) {
                            String impu = p.get("IMPU");
                            if (impu != null) {
                                String proto = impu.substring(0, 3);
                                event.set(event.get() + '_' + proto);
                            }
                        } else {
                            String ei = p.get("ei");
                            if (ei != null)
                                event.set(event.get() + '_' + ei);
                        }
                    }
                    String id = p.get("IMSI");
                    if (id == null) id = p.get("assocId");
                    if (id == null) id = p.get("mscId");
                    System.out.println(id + " -> " + event.get());

                    return event.get();
                }, p -> {
                    String[] h = headers.get(event.get());
                    LinkedHashMap<String, Object> r = new LinkedHashMap<>();
                    for (String f : h) r.put(f, "");
                    r.putAll(p);
                    return r;
                }));

        record.forEach((key, value) -> {
            try {
//                System.out.println("\n-------\nPrint Keys -> " + key);
//                System.out.println("\n\nPrint Values ->\n" + value);
                handleEvents(key, value);
//                System.out.println("\n---------\n");
            } catch (Exception ex) {
                ex.printStackTrace();
                throw new RuntimeException(ex);
            }
        });
        return new LinkedHashMap<>(record);
    }

    private List<List<String>> getBlock() throws IOException {
        blockNo++;

        List<String> sBlock = new ArrayList<>();
        sBlock.add(carryForwardLine);      // get dn: from hasNext, look ahead read
        LinkedHashMap<String, String> dn = commaSeparatedProperties(carryForwardLine);

        String id = dn.get("mscId");
        if (id == null)
            id = dn.get("assocId");
        String subsIdentity = id;

        hasNext = false;
        List<List<String>> block = new ArrayList<>();
        for (String line; (line = fileReader.readLine()) != null; ) {
            lineNo++;
            line = line.trim();
            if (line.equals("")) continue;
            if (line.startsWith("dn:")) {
                String line1 = line.substring(line.indexOf(':') + 1);
                LinkedHashMap<String, String> r = commaSeparatedProperties(line1);

                id = r.get("mscId");
                if (id == null)
                    id = r.get("assocId");

                if (id.equals(subsIdentity)) {
                    block.add(sBlock);
                    sBlock = new ArrayList<>();
                    sBlock.add(line);
                    continue;
                } else {
                    carryForwardLine = line;
                    hasNext = true;
                }
                return block;
            } else
                sBlock.add(line);
        }
        return block;
    }

    private void processRecord(LinkedHashMap<String, Object> record, IEnrichment enrichment) throws Exception {
    }

    private static Map<String, String> createRecord(List<String> sBlock) {
        LinkedHashMap<String, HashSet<String>> rec = new LinkedHashMap<>();
        sBlock.forEach(line -> {
            if (line.startsWith("dn:") || line.startsWith("aliasedObjectName:")) {

                line = line.substring(line.indexOf(':') + 1);
                LinkedHashMap<String, String> r = commaSeparatedProperties(line);
                String mscId = r.get("mscId");
                if (mscId != null && mscId.length() > 15) {
                    String imsi = mscId.substring(mscId.length() - 15);
                    if (StringUtils.isNumeric(imsi))
                        r.put("IMSI", imsi);
                }

                r.forEach((key, value) -> {
                    HashSet<String> valSet = rec.get(key);
                    if (valSet == null) valSet = new HashSet<>();
                    valSet.add(value);
                    rec.put(key, valSet);
                });
            } else {
                String k = line.substring(0, line.indexOf(':'));
                String v = line.substring(line.indexOf(':') + 1).trim();
                HashSet<String> valSet = rec.computeIfAbsent(k, k1 -> new HashSet<>());
                valSet.add(v);
            }
        });

        return rec.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, k -> String.join("|", k.getValue())));
    }

    private static LinkedHashMap<String, String> commaSeparatedProperties(String line) {
        LinkedHashMap<String, String> m = new LinkedHashMap<>();
        String[] props = line.split(",");
        for (String prop : props) {
            String[] keyVal = prop.split("=");
            String key = keyVal[0].trim().replace('-', '_');
            m.put(key, keyVal[1].trim());
        }
        return m;
    }

    public void close() throws IOException {
//        sourceFileReader.close();
    }

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {
    }
}