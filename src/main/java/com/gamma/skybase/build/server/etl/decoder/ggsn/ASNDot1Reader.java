package com.gamma;

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
public class ASNDot1Reader extends TLReader {
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
        while (executor.hasNext()) {
            Map<String, Object> r = executor.next();
            System.out.println(r);
        }
        executor.close();
        System.out.println("");
    }

    public Map<String, Object> next() throws Exception {
//           int hex = skipUntil(new int[]{0xBF});
//        String hex = skipUntil(new int[]{0xBF});
//        String he x= skipFiller(0xBF);

        String hex = skipBytes(4);
        System.out.println("Skipped " + hex);

        Tag tag = readTag("");
        int length = readLength();
        byte[] value = new byte[length];
        read(value);
        LinkedHashMap<String, Object> record = new LinkedHashMap<>();
        TagReader tr = new TagReader(String.valueOf(tag.tValue), value);
//        Map<String, Object> xx = tr.parse();

//        while (tr.hasNext()) {
//            if (tag.constructed) {
//                T tlv = tr.readTag("");
//                int l = getLength();
//                byte[] v = new byte[l];
//                ArrayList<T> sNode = new TagReader(String.valueOf(tlv.tValue), v).parse();
//                record.put(String.valueOf(tag.tValue), sNode);
//            } else
//                record.put(String.valueOf(tag.tValue), value);
//        }

//            System.out.println("V:" + printHexBinary(buffer));
//            if(!tag.constructed)
//            System.out.println("T:" + tag.value + "\tL:" + length + "\tC:" + tag.constructed + "\tO:" + offset);

        return tr.parse();
    }

    public boolean hasNext() throws IOException {
        return bis.available() > 0;
    }

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

    public String skipUntil(int[] tags) throws IOException {
        bis.mark(16);
        int count = 0;
        byte[] value = new byte[0];
        boolean skip = true;
        while (skip) {
            count++;
            int c = read();
            for (int v : tags)
                if (v == c) {
                    bis.reset();
                    value = new byte[count];
                    read(value);
                    skip = false;
                    break;
                }
        }
        return printHexBinary(value).toUpperCase();
    }

    @Override
    public String skipFiller(int filler) throws IOException {
        bis.mark(16);
        byte[] value = new byte[0];
        int count = 0;
        boolean skip = true;
        while (skip) {
            count++;
            if (read() == filler) {
                bis.reset();
                value = new byte[count];
                read(value);
                skip = false;
//                break;
            }
        }
        return printHexBinary(value).toUpperCase();
    }

    public void close() throws IOException {
        if (bis != null)
            bis.close();
    }

    private static class TagReader extends TLReader {
        byte[] data;
        int offset = 0;

        public TagReader(String valueOf, byte[] value) {
            data = value;
            this.pTag = pTag;
        }

        protected boolean hasNext() {
            return  data.length > offset;
        }

        public Map<String, Object> parse() throws Exception {
            Map<String, Object> nodes = new LinkedHashMap<>();
            while (hasNext()) {

//                String ignored = ignoreFiller(0x00);
//                String skip = skipBytes(4);

                Tag t = readTag(pTag);
                int length = readLength();
                byte[] buf = new byte[length];
                read(buf);
                String tagNo = t.tValue + "";
                if (t.constructed) {
                    switch (t.clazz) {
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

                    Map<String, Object> x = new TagReader(tagNo, buf).parse();
                    Object val = nodes.get(tagNo);
                    if (val != null) {
                        if (val instanceof List)
                            ((ArrayList) val).add(x);
                        else {
                            List<Object> l = new ArrayList<>();
                            l.add(val);
                            nodes.put(tagNo, x);
                        }
                    }
                    else nodes.put(tagNo, x);
                } else
                    nodes.put(tagNo, Decoder.getValue(tagNo, buf));

//                System.out.println("\t\tT: " + tag.pTag + "\t\tL:" + length + "\t\tC:" + tag.constructed
//                        + "\t\tO:" + offset + "\t\tV:" + tag.getValue() + "\t(" + printHexBinary(buf) + ")");

//                if (!node.constructed)
//                System.out.println("\tP: " + node.pTag + ",\t\tT: " + node.tValue + ",\tL: " + length + ",\tO:"
//                        + offset + ",\tV: " + Decoder.getValue(tagNo, data) + ",\t\t(" + printHexBinary(buf) + ")");
//                nodes.put(node);
            }
            return nodes;
        }

        public String skipFiller(int filler) throws IOException {
            int c = read();
            StringBuilder hex = new StringBuilder();
            while (c == filler) {
                hex.append(" ").append(Integer.toHexString(c).toUpperCase()).append(" ");
                c = read();
            }
            offset--;
            hex.append("|").append(Integer.toHexString(c).toUpperCase());
            System.out.println(hex);
            if (c == -1) throw new EOFException();
            return hex.toString();
        }

        @Override
        public String skipUntil(int[] tags) throws IOException {
            int c;
            StringBuilder hex = new StringBuilder();
            boolean skip = true;
            while (skip) {
                c = read();
                for (int v : tags) {
                    if (v == c) {
                        offset--;
                        skip = false;
                        break;
                    }
                    hex.append(Integer.toHexString(c).toUpperCase()).append(" ");
                }
            }
            return hex.toString();
        }

        public int read() throws EOFException {
            if (offset >= data.length) throw new EOFException();
            int result = data[offset];
            offset++;
            return result;
        }

        public void read(byte[] buf) throws EOFException {
            if (offset >= data.length) throw new EOFException();
            System.arraycopy(data, offset, buf, 0, buf.length);
            offset = offset + buf.length;
        }
    }

}

class Decoder {
    private static Map<String, TagProps> decoderMap = new HashMap<>();

    static {
        decoderMap.put("79.0", new TagProps("recordType", "INTEGER"));
    }

    public static Object getValue(String pTag, byte[] data) {
        Object v = "";
        if (data != null) {
            TagProps dConf = decoderMap.get(pTag);

            if (dConf != null) {
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

    public static String toBCDString(byte[] bValue) {
        StringBuilder strInt = new StringBuilder();
        for (byte t_byte : bValue) { // take a nibble
            int m_nibble = t_byte & 0xF0;
            m_nibble = m_nibble >>> 4;
            if (m_nibble < 0x0A) // since it is a BCD string
                strInt.append(m_nibble);
            int l_nibble = t_byte & 0x0F;

            if (l_nibble < 0x0A) // since it is a BCD string
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

    static class TagProps {
        public String name, method;

        public TagProps(String name, String method) {
            this.name = name;
            this.method = method;
        }
    }
}


abstract class TLReader {
    String pTag;
    static char[] hexCode = "0123456789ABCDEF".toCharArray();

    public abstract int read() throws IOException;

    public abstract String skipUntil(int[] tags) throws IOException;

    public abstract String skipFiller(int filler) throws IOException;

    public String skipBytes(int count) throws IOException {
        StringBuilder hex = new StringBuilder();
        for (int i = 0; i < count; i++) {
            int c = read();
            hex.append(Integer.toHexString(c).toUpperCase()).append(" ");
            if (c == -1) throw new EOFException();
        }
        System.out.println(hex);
        return hex.toString();
    }

    Tag readTag(String pTag) throws IOException {
        this.pTag = pTag;
        StringBuilder hex = new StringBuilder();
        int c = read();
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

    int readLength() throws Exception {
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
    public int clazz, tValue;
    public boolean explicit, constructed;
    public String pTag = ""; // the parent tags
    //    public Map<String, Object> sNodes = new LinkedHashMap<>(); // the tag's actual value


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
