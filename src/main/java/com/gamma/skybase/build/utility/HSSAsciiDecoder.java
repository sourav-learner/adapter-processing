package com.gamma.skybase.build.utility;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

@Slf4j
public class HSSAsciiDecoder implements Closeable {

    /**
     * The default line to start reading.
     */
    public static final int DEFAULT_SKIP_LINES = 0;
    final BufferedReader br;
    private String lookAheadLine = null;
    long lineNo = 0;
    long blockNo = 0;
    long recordNo = 0;

    String[] headers = {"ESMUSERPROFILEID", "ESMIMSI", "ESMMSISDN"};

    public HSSAsciiDecoder(String filePath) throws IOException {
        br = Files.newBufferedReader(Paths.get(filePath), StandardCharsets.UTF_8);
        readHeaders();
    }

    private void readHeaders() throws IOException {
        for (String line; (line = br.readLine()) != null; ) {
            lineNo++;
            line = line.trim();
            if (line.equals("")) break;
        }
    }

//    public static void main(String[] args) throws IOException {
//        HSSAsciiDecoder hssAsciiDecoder = new HSSAsciiDecoder("C:\\projects\\skybase-zain-sudan-build\\USERprofileDump16122021.txt");
//        String ofile = "C:\\projects\\skybase-zain-sudan-build\\USERprofileDump16122021.csv";
//
//        BufferedWriter bw = Files.newBufferedWriter(Paths.get(ofile), StandardCharsets.UTF_8);
//        String header = "";
//        for (String f : hssAsciiDecoder.headers) {
//            if (header.equals("")) header = f;
//            else
//                header = header + ", " + f;
//        }
//        bw.write(header + "\n");
//        while (hssAsciiDecoder.hasNext()) {
//            LinkedHashMap<String, Object> record = hssAsciiDecoder.next();
//            Collection<Object> values = record.values();
//            String value = "";
//            for (Object f : values) {
//                if (value.equals("")) value = f.toString();
//                else
//                    value = value + ", " + f.toString();
//            }
//            bw.write(value + "\n");
//            System.out.println(value);
//        }
//    }

    LinkedHashMap<String, Object> lastRecord;

    public boolean hasNext() throws IOException {
        lastRecord = null;
        LinkedHashMap<String, Object> r = new LinkedHashMap<>();
        List<String> block = getBlock();
        if (block != null) {
            r = createRecord(block);
        }

        if (!r.isEmpty()) {
            lastRecord = r;
        }
        return lastRecord != null;
    }

    private LinkedHashMap<String, Object> createRecord(List<String> block) {
        LinkedHashMap<String, Object> record = new LinkedHashMap<>();
        for (String line : block) {
            String[] kv = line.split(":");
            if (kv.length > 1) {
                String k = kv[0].trim();
                if (k.contains("-"))
                    k = k.substring(k.indexOf('-') + 1);
                try {
                    record.put(k.toUpperCase(), kv[1].trim());
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
        return record;
    }

    private List<String> getBlock() throws IOException {
        List<String> block = new ArrayList<>();
        boolean isStart = false;
        String dnLine = "";
        for (String line; (line = br.readLine()) != null; ) {
            lineNo++;
            line = line.trim();

            if (line.equals(""))
                if (!isStart)
                    continue;
                else if(isValid(block))
                    return block;
                else {
                    isStart = false;
                    block = new ArrayList<>();
                }

            if (line.startsWith("HSS-EsmUserProfileId") || line.startsWith("HSS-EsmImsi") || line.startsWith("HSS-EsmMsisdn")) {
                block.add(line);
                isStart = true;
            }
        }
        blockNo++;
        return null;
    }

    private boolean isValid(List<String> block) {
        return block.size() == 3;
    }

    private LinkedHashMap<String, Object> parseDN(String line) {
        LinkedHashMap<String, Object> m = new LinkedHashMap<>();
        if (line.startsWith("dn:"))
            line = line.substring(3);
        String[] props = line.split(",");

        for (String prop : props) {
            String[] keyVal = prop.split("=");
            String key = keyVal[0].trim().replace('-', '_');
            m.put(key, keyVal[1].trim());
        }
        return m;
    }

    public LinkedHashMap<String, Object> next() throws IOException {
        recordNo++;
        return lastRecord;
    }

    public void close() throws IOException {
        br.close();
    }
}
