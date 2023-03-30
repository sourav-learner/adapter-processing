package com.gamma.skybase.build.server.etl.decoder.ggsn;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Unit test for simple App.
 */
public class ASNDot1Reader extends TAGReader {
    String fileName;
    BufferedInputStream bis;
    long offset;
    int size;

    public ASNDot1Reader(String fileName) {
        try {
            this.fileName = fileName;
            FileInputStream fis = new FileInputStream(fileName);
            bis = new BufferedInputStream(fis);
            size = bis.available();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static void main(String[] args) throws Exception {
        String filename = "C:\\sandbox\\asn-decoders\\huawei-gsn-lebara\\data\\L1CG1_FILE20220901000000_9640.dat";
//      String filename = "C:\\sandbox\\incubator\\gasn\\gasn\\data\\sudan_south\\ggsn\\PSPGW2022091000213111";

        ASNDot1Reader executor = new ASNDot1Reader(filename);
        executor.parseFile();
        System.out.println("");
    }

    public void parseFile() throws Exception {
        while (!hasNext())
            next();
        close();
    }

    public LinkedHashMap<String, Object> next() throws Exception {
        Tag tag = skipUntil(0xBF);
        int length = getLength();
        byte[] buffer = new byte[length];
        read(buffer);
        LinkedHashMap<String, Object> record = new LinkedHashMap<>();
        if (tag.constructed) {
            ArrayList<Tag> sNode = new NodeDecoder().parse(String.valueOf(tag.tValue), buffer);
            record.put(String.valueOf(tag.tValue), sNode);
        } else
            record.put(String.valueOf(tag.tValue), buffer);

//            System.out.println("V:" + printHexBinary(buffer));
//            if(!tag.constructed)
//            System.out.println("T:" + tag.value + "\tL:" + length + "\tC:" + tag.constructed + "\tO:" + offset);
        return record;
    }

    public boolean hasNext() throws IOException {
        return bis.available() > 0;
    }

//    long skipOffset(int size) throws IOException {
//        this.offset = this.offset + size;
//        return this.bis.skip(size);
//    }

    public int read() throws IOException {
        offset++;
        int result = bis.read();
        if (result == -1) throw new EOFException();
        return result;
    }

    public void read(byte[] buffer) throws IOException {
        offset = offset + buffer.length;
        int result = bis.read(buffer);
        if (result == -1) throw new EOFException();
    }


    public Tag skipUntil(int filler) throws IOException {
        int c = read();
        StringBuilder hex = new StringBuilder();
        while (c != filler) {
            hex.append(Integer.toHexString(c).toUpperCase()).append(" ");
            c = read();
        }
        hex.append(" | ").append(Integer.toHexString(c).toUpperCase());
        System.out.println(hex);
        return getTag(pTag, c);
    }

    public void close() throws IOException {
        if (bis != null)
            bis.close();
    }


    private static class NodeDecoder extends TAGReader {
        byte[] buffer;
        int offset = 0;


        public ArrayList<Tag> parse(String pTag, byte[] b) throws Exception {
            buffer = b;
            this.pTag = pTag;
            ArrayList<Tag> nodes = new ArrayList<>();
            while (offset < buffer.length) {
                Tag node = readTagIgnoreFiller(pTag, 0x00);
                int length = getLength();
                byte[] buf = new byte[length];
                read(buf);
                if (node.constructed) {
                    switch (node.clazz) {
                        case 0:
                            System.out.println("<UNIVERSAL>");
                            break;
                        case 64:
                            System.out.println("<APPLICATION>");
                            break;
                        case 128:
                            System.out.println("<CONTEXT>");
                            break;
                        case 192:
                            System.out.println("<PRIVATE>");
                            break;
                        default:
                            System.out.println("<UNKNOWN>");
                    }
                    ArrayList<Tag> x = new NodeDecoder().parse(pTag + '.' + node.tValue, buf);
                    node.sNodes.add(x);
                } else {
                    node.data = node.getValue(buffer);
                }
//                System.out.println("\t\tT: " + tag.pTag + "\t\tL:" + length + "\t\tC:" + tag.constructed
//                        + "\t\tO:" + offset + "\t\tV:" + tag.getValue() + "\t(" + printHexBinary(buf) + ")");
//                if (!node.constructed)
                System.out.println("\tP: " + node.pTag + ",\t\tT: " + node.tValue + ",\tL: " + length + ",\tO:"
                        + offset + ",\tV: " + node.getValue(buffer) + ",\t\t(" + printHexBinary(buf) + ")");
                nodes.add(node);
            }
            return nodes;
        }

        public Tag readTagIgnoreFiller(String pTag, int filler) throws IOException {
            int c = read();
            StringBuilder hex = new StringBuilder();
            while (c == filler) {
                hex.append(" ").append(Integer.toHexString(c).toUpperCase()).append(" ");
                c = read();
            }
            hex.append("|").append(Integer.toHexString(c).toUpperCase());
            System.out.println(hex);
            if (c == -1) throw new EOFException();
            return getTag(pTag, c);
        }

        public int read() throws EOFException {
            if (offset >= buffer.length) throw new EOFException();
            int result = buffer[offset];
            offset++;
            return result;
        }

        public void read(byte[] buf) throws EOFException {
            if (offset >= buffer.length) throw new EOFException();
            System.arraycopy(buffer, offset, buf, 0, buf.length);
            offset = offset + buf.length;
        }
    }

}

class Decoder {
    public static String toBCDString(byte[] bvalue) {
        StringBuilder strInt = new StringBuilder();
        for (byte t_byte : bvalue) {//take a nibble
            int m_nibble = t_byte & 0xF0;
            m_nibble = m_nibble >>> 4;
            if (m_nibble < 0x0A) //since it is a BCD string
                strInt.append(m_nibble);
            int l_nibble = t_byte & 0x0F;

            if (l_nibble < 0x0A)//since it is a BCD string
                strInt.append(l_nibble);
        }
        return strInt.toString();
    }

    public static String toTimeStamp(byte[] ba) {
        StringBuilder time = new StringBuilder();
        for (int i = 0; i < ba.length; i++) {
            if (i == 6) {
                Date d = new Date(Long.parseLong(time.toString()));
                if ((char) ba[i] == '0')
                    time.append(" +");
                else if ((char) ba[i] == '1')
                    time.append(" -");
                else
                    time.append(" +");
            } else {
                time.append((ba[i] & 0xF0) >>> 4);
                time.append(ba[i] & 0x0F);
            }
        }
        return time.toString();
    }

    public static String toPDPType(byte[] hexValues) {
        if (hexValues.length == 1) {
            return String.valueOf(hexValues[0]);
        } else if (hexValues.length > 1) {
            return String.valueOf(hexValues[0]) + hexValues[1];
        }
        return "";
    }

    public static long toLong(byte[] data) throws NumberFormatException {
        long lNum = 0;
        for (byte datum : data) {
            lNum = lNum * 16 + ((datum & 0xF0) >>> 4);
            lNum = lNum * 16 + (datum & 0x0F);
        }
        return lNum;
    }

    public static String IPV4Address(byte[] data) {
        try {
            return InetAddress.getByAddress(data).getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
//            throw new RuntimeException(e);
        }
        return "";
    }

    public static String TBCD(byte[] data) {
        StringBuilder tbcd = new StringBuilder();
        for (int i = 0; i < data.length - 1; i++) {
            tbcd.append(data[i] & 0x0F);
            tbcd.append((data[i] & 0xF0) >>> 4);
        }
        tbcd.append(data[data.length - 1] & 0x0F);
//        if (oeFlag == 1)
//            tbcd.append(data[data.length - 1] & 0x0F);
//        else {
//            tbcd.append(data[data.length - 1] & 0x0F);
//            tbcd.append((data[data.length - 1] & 0xF0) >>> 4);
//        }
        return tbcd.toString();
    }
}


abstract class TAGReader {
    String pTag;

    public abstract int read() throws IOException;

    Tag getTag(String pTag, int c) throws IOException {
        this.pTag = pTag;
        StringBuilder hex = new StringBuilder();
        int tClass = c & 0xC0;
        boolean tConstructed = (c & 0x20) != 0;
        int tValue = c & 0x1F;
        if (tValue == 0x1F) { // multiple bytes for tag number
            c = read();
            hex.append(" ").append(Integer.toHexString(c).toUpperCase());
            tValue = c & 0x7F;
            int[] appArr = new int[4];//assuming that APP ID would not be greater than (2^28=)268435456
            appArr[0] = tValue;

            int i = 1; // octet counter
            while ((c & 0x80) != 0) {
                c = read();
                hex.append(" ").append(Integer.toHexString(c).toUpperCase());
                tValue = c & 0x7F;
                appArr[i++] = tValue;
            }
            tValue = calculateAppNumber(appArr, i);//ns
        }
        if (this.pTag != null && this.pTag.length() > 0)
            this.pTag = this.pTag + '.' + tValue;
        else this.pTag = "";

        Tag result = new Tag(this.pTag, tClass, tValue, true, tConstructed);
        System.out.print("\nHex:" + hex + " \t| ");
        return result;
    }

    int getLength() throws Exception {
        int limit = read();

        StringBuilder hex = new StringBuilder(Integer.toHexString(limit).toUpperCase());

        int result;
        if ((limit & 0x80) == 0) result = limit;
        else if (limit == 0x80) // NON-definite Length encoding
            return -1;
        else {
            limit &= 0x7F;
            if (limit > 4) throw new Exception();
            result = 0;
            while (limit-- > 0) {
                int c = read();
                hex.append(" ").append(Integer.toHexString(c).toUpperCase());
                result = (result << 8) | (c & 0xFF);
            }
        }
        System.out.println("L: " + hex);
        return result;
    }


    private static final char[] hexCode = "0123456789ABCDEF".toCharArray();

    public static String printHexBinary(byte[] data) {
        StringBuilder r = new StringBuilder(data.length * 2);
        for (byte b : data) {
            r.append(hexCode[(b >> 4) & 0xF]);
            r.append(hexCode[(b & 0xF)]);
            r.append(" ");
        }
        return r.toString();
    }

    static int calculateAppNumber(int[] number, int octet) {
        int s2power = 1, result = 0;

        for (int i = 0; i < octet; i++) {
            String binStr = Integer.toBinaryString(number[octet - i - 1]);
            HashMap<String, Integer> hm = calculateInt(binStr, s2power, result);
            Integer keep = hm.get("POWER");
            int e2power = keep;
            keep = hm.get("RESULT");
            result = keep;
            s2power = e2power;//for next iteration
        }
        return result;
    }

    static HashMap<String, Integer> calculateInt(String p_binStr, int p_2power, int p_result) {
        int sum = p_result;
        int strLen = p_binStr.length();
        int _2power = p_2power;

        for (int i = 0; i < 7; i++) { //for 7 bits
            if (i < strLen) {
                int bitvalue = p_binStr.charAt(strLen - i - 1);
                if (bitvalue == 48) //char for 0 is 48
                    bitvalue = 0;
                else bitvalue = 1;//when bitvalue=49
                sum += (bitvalue * _2power); //power of 2
            }
            _2power *= 2;
        }
        HashMap<String, Integer> ret = new HashMap<>();
        ret.put("POWER", _2power);
        ret.put("RESULT", sum);
        return ret;
    }

}

class Tag {
    public String pTag = ""; // the parent tags
    public int clazz; // the tag's class id
    public int tValue; // the tag's actual value
    public Object data; // the tag's actual value
    public ArrayList<ArrayList<Tag>> sNodes = new ArrayList<>(); // the tag's actual value
    public boolean explicit; // is it explicit or implicit?
    public boolean constructed; // is it constructed?
    private static Map<String, TagConf> decoderMap = new HashMap<>();

    static {
        decoderMap.put("79.0", new TagConf("recordType", "INTEGER"));
        decoderMap.put("79.3", new TagConf("servedIMSI", "tbcd"));
        decoderMap.put("79.4.0", new TagConf("pGWAddress", "IPV4_ADDRESS"));
        decoderMap.put("79.5", new TagConf("chargingID", "INTEGER"));
        decoderMap.put("79.6.0", new TagConf("servingNodeAddress", "IPV4_ADDRESS"));
        decoderMap.put("79.7", new TagConf("accessPointNameNI", "OCTET_STRING"));
        decoderMap.put("79.8", new TagConf("pdpPDNType", "PDP"));
        decoderMap.put("79.9.0.0", new TagConf("servedPDPPDNAddress", "IPV4_ADDRESS"));
        decoderMap.put("79.11", new TagConf("dynamicAddressFlag", "INTEGER"));
        decoderMap.put("79.12.16.1", new TagConf("qosRequested", "OCTET_STRING"));
        decoderMap.put("79.12.16.2", new TagConf("qosNegotiated", "OCTET_STRING"));
        decoderMap.put("79.12.16.3", new TagConf("dataVolumeGPRSUplink", "INTEGER"));
        decoderMap.put("79.12.16.4", new TagConf("dataVolumeGPRSDownlink", "INTEGER"));
        decoderMap.put("79.12.16.5", new TagConf("changeCondition", "INTEGER"));
        decoderMap.put("79.12.16.6", new TagConf("changeTime", "TIMESTAMP"));
        decoderMap.put("79.12.16.7", new TagConf("failureHandlingContinue", "OCTET_STRING"));
        decoderMap.put("79.12.16.8", new TagConf("userLocationInformation", "OCTET_STRING"));
        decoderMap.put("79.12.16.9.1", new TagConf("qCI", "INTEGER"));
        decoderMap.put("79.12.16.9.2", new TagConf("maxRequestedBandwithUL", "INTEGER"));
        decoderMap.put("79.12.16.9.3", new TagConf("maxRequestedBandwithDL", "INTEGER"));
        decoderMap.put("79.12.16.9.4", new TagConf("guaranteedBitrateUL", "INTEGER"));
        decoderMap.put("79.12.16.9.5", new TagConf("guaranteedBitrateDL", "INTEGER"));
        decoderMap.put("79.12.16.9.6", new TagConf("aRP", "INTEGER"));
        decoderMap.put("79.12.16.9.7", new TagConf("aPNAggregateMaxBitrateUL", "INTEGER"));
        decoderMap.put("79.12.16.9.8", new TagConf("aPNAggregateMaxBitrateDL", "INTEGER"));
        decoderMap.put("79.12.16.9.9", new TagConf("extendedMaxRequestedBWUL", "INTEGER"));
        decoderMap.put("79.12.16.9.10", new TagConf("extendedMaxRequestedBWDL", "INTEGER"));
        decoderMap.put("79.12.16.9.11", new TagConf("extendedGBRUL", "INTEGER"));
        decoderMap.put("79.12.16.9.12", new TagConf("extendedGBRDL", "INTEGER"));
        decoderMap.put("79.12.16.9.13", new TagConf("extendedAPNAMBRUL", "INTEGER"));
        decoderMap.put("79.12.16.9.14", new TagConf("extendedAPNAMBRDL", "INTEGER"));
        decoderMap.put("79.12.16.10", new TagConf("", "OCTET_STRING"));
        decoderMap.put("79.12.16.11", new TagConf("", "OCTET_STRING"));
        decoderMap.put("79.12.16.12", new TagConf("", "OCTET_STRING"));
        decoderMap.put("79.12.16.13", new TagConf("", "OCTET_STRING"));
        decoderMap.put("79.12.16.14", new TagConf("", "OCTET_STRING"));
        decoderMap.put("79.12.16.15", new TagConf("", "OCTET_STRING"));
        decoderMap.put("79.13", new TagConf("recordOpeningTime", "BCD"));
        decoderMap.put("79.14", new TagConf("duration", "INTEGER"));
        decoderMap.put("79.15", new TagConf("causeForRecClosing", "INTEGER"));
        decoderMap.put("79.16.0", new TagConf("gsm0408Cause", "INTEGER"));
        decoderMap.put("79.16.1", new TagConf("gsm0902MapErrorValue", "INTEGER"));
        decoderMap.put("79.16.2", new TagConf("tu_tQ767Cause", "INTEGER"));
        decoderMap.put("79.16.3", new TagConf("networkSpecificCause", "OCTET_STRING"));
        decoderMap.put("79.16.4", new TagConf("manufacturerSpecificCause", "INTEGER"));
        decoderMap.put("79.16.5", new TagConf("positionMethodFailureCause", ""));
        decoderMap.put("79.16.6", new TagConf("unauthorizedLCSClientCause", ""));
        decoderMap.put("79.17", new TagConf("recordSequenceNumber", ""));
        decoderMap.put("79.18", new TagConf("nodeID", ""));
        decoderMap.put("79.19", new TagConf("recordExtensions", ""));
        decoderMap.put("79.20", new TagConf("localSequenceNumber", ""));
        decoderMap.put("79.21", new TagConf("apnSelectionMode", ""));
        decoderMap.put("79.22", new TagConf("servedMSISDN", ""));
        decoderMap.put("79.23", new TagConf("chargingCharacteristics", ""));
        decoderMap.put("79.24", new TagConf("chChSelectionMode", ""));
        decoderMap.put("79.25", new TagConf("iMSsignalingContext", ""));
        decoderMap.put("79.26", new TagConf("externalChargingID", ""));
        decoderMap.put("79.27", new TagConf("servingNodePLMNIdentifier", ""));
        decoderMap.put("79.28", new TagConf("pSFurnishChargingInformation", ""));
        decoderMap.put("79.29", new TagConf("servedIMEISV", ""));
        decoderMap.put("79.30", new TagConf("rATType", ""));
        decoderMap.put("79.31", new TagConf("mSTimeZone", ""));
        decoderMap.put("79.32", new TagConf("userLocationInformation", ""));
        decoderMap.put("79.33", new TagConf("cAMELChargingInformation", "CHARACTER_STRING"));
        decoderMap.put("79.34.16.0", new TagConf("", ""));
        decoderMap.put("79.34.16.1", new TagConf("", ""));
        decoderMap.put("79.34.16.2", new TagConf("", ""));
        decoderMap.put("79.34.16.3", new TagConf("", ""));
        decoderMap.put("79.34.16.4", new TagConf("", ""));
        decoderMap.put("79.34.16.5", new TagConf("", ""));
        decoderMap.put("79.34.16.6", new TagConf("", ""));
        decoderMap.put("79.34.16.7", new TagConf("", ""));
        decoderMap.put("79.34.16.8", new TagConf("", ""));
        decoderMap.put("79.34.16.9", new TagConf("", ""));
        decoderMap.put("79.34.16.9.1", new TagConf("", ""));
        decoderMap.put("79.34.16.9.6", new TagConf("", ""));
        decoderMap.put("79.34.16.10", new TagConf("", ""));
        decoderMap.put("79.34.16.10.0", new TagConf("", ""));
        decoderMap.put("79.34.16.11", new TagConf("", ""));
        decoderMap.put("79.34.16.12", new TagConf("", ""));
        decoderMap.put("79.34.16.13", new TagConf("", ""));
        decoderMap.put("79.34.16.14", new TagConf("", ""));
        decoderMap.put("79.34.16.15", new TagConf("", ""));
        decoderMap.put("79.35.10", new TagConf("", ""));
        decoderMap.put("79.35.0", new TagConf("", ""));
    }

    /**
     * @param clazz       the tag's class.
     * @param tValue      the tag's value.
     * @param explicit    whether this tag is explicit or implicit. Default is explicit.
     * @param constructed Whether this tag is constructed or not. Default is not constructed
     */
    public Tag(String pTag, int clazz, int tValue, boolean explicit, boolean constructed) {
        this.pTag = pTag;
        this.clazz = clazz;
        this.tValue = tValue;
        this.explicit = explicit;
        this.constructed = constructed;

    }

    public Object getValue(byte[] data) {
        Object v = "";
        if (data != null) {
            TagConf dConf = decoderMap.get(pTag);
            if (!constructed)
                if (dConf != null) {
                    v = dConf.name + "-> ";
                    switch (dConf.method.toUpperCase()) {
                        case "OCTET_STRING":
                            v = new String(data, StandardCharsets.UTF_8);
                            break;
                        case "TBCD":
                            v = Decoder.TBCD(data);
                            break;
                        case "IPV4_ADDRESS":
                            v = Decoder.IPV4Address(data);
                            break;
                        case "LONG":
                            v = Decoder.toLong(data);
                            break;
                        case "PDP":
                            v = Decoder.toPDPType(data);
                            break;
                        case "TIMESTAMP":
                            v = Decoder.toTimeStamp(data);
                            break;
                        case "BCD":
                            v = Decoder.toBCDString(data);
                            break;
                        case "INTEGER":
                            v = new BigInteger(data);
                            break;
                        default:
                            v = data;
                    }
                } else
                    return pTag + new String(data, StandardCharsets.UTF_8);
        }
        return v;
    }
    public String toString() {
        return "<Tag class=\"" + clazz + "\" value=\"" + tValue + "\" explicit=\"" + explicit + "\" constructed=\"" + constructed + "\" />";
    }

}


//class TagConst {
//    /**
//     * Constant values for Tag classes.
//     */
//    public static final int UNIVERSAL = 0x00;
//    public static final int APPLICATION = 0x40;
//    public static final int CONTEXT = 0x80;
//    public static final int PRIVATE = 0xC0;
//
//    /**
//     * Constant values for Tag values.
//     */
//    public static final int BOOLEAN = 0x01;
//    public static final int INTEGER = 0x02;
//    public static final int BIT_STRING = 0x03;
//    public static final int OCTET_STRING = 0x04;
//    public static final int NULL = 0x05;
//    public static final int OBJECT_IDENTIFIER = 0x06;
//    public static final int REAL = 0x09;
//
//    public static final int SEQUENCE = 0x10; // 16
//    public static final int SEQUENCE_OF = 0x10; // 16
//    public static final int SET = 0x11; // 17
//    public static final int SET_OF = 0x11; // 17
//
//    public static final int NUMERIC_STRING = 0x12; // 18
//    public static final int PRINTABLE_STRING = 0x13; // 19
//    public static final int T61_STRING = 0x14; // 20
//    public static final int VIDEOTEX_STRING = 0x15; // 21
//    public static final int IA5_STRING = 0x16; // 22
//    public static final int GRAPHIC_STRING = 0x19; // 25
//    public static final int ISO646_STRING = 0x1A; // 26
//    public static final int GENERAL_STRING = 0x1B; // 27
//    public static final int UNIVERSAL_STRING = 0x1C; // 28
//    public static final int BMP_STRING = 0x1E; // 30
//    public static final int UTC_TIME = 0x17; // 23
//    public static final int GENERALIZED_TIME = 0x18; // 24
//}
