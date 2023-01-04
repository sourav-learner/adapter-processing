package com.gamma.skybase.build.server.etl.tx.ccn;

import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by abhijit on 27/8/14.
 */
public class AccumulatorsHandler {

    private final Map<String, List<Map<String, Object>>> accuAccounts = new LinkedHashMap<>();
    private int maxNumberOfAccumulators = 5;

    public void setAccuAccount(List<Map<String, Object>> param) {
        List<List<Object>> accumulators = param.stream().filter(e -> e.containsKey("accumulators"))
                .map(e -> (List<Object>) e.get("accumulators"))
                .collect(Collectors.toList());

        accumulators.forEach((accs) -> {
            accs.forEach((accum) -> {
                Map<String, Object> accumulator = ((ArrayList<Map<String, Object>>) accum).stream().findFirst().get();
                Object accumulatorID = accumulator.get("accumulatorID");
                if (accumulatorID != null) {
                    List<Map<String, Object>> existing = accuAccounts.get(accumulatorID.toString().trim());
                    if (existing != null) {
                        existing.add(accumulator);
                    } else {
                        existing = new ArrayList<>();
                        existing.add(accumulator);
                        accuAccounts.put(accumulatorID.toString().trim(), existing);
                    }
                }
            });
        });
    }

    public Map<String, String> handleAccumulators2() {
        Map<String, String> m = new LinkedHashMap<>();
        Set<Map.Entry<String, List<Map<String, Object>>>> accountNo = accuAccounts.entrySet();

        Set<String> accumulatorIDUsedset = accuAccounts.keySet();
        String k = "";
        for (String s : accumulatorIDUsedset)
            k = k + ',' + s;
        if (k.length() > 1) k = k.substring(1);

        int c = 1;
        for (List<Map<String, Object>> legs : accuAccounts.values()) {
            Object max = max(legs, "accumulatorBefore");
            String bef = "ACCUMULATOR_" + c+"_VALUE_BEF";
            m.put(bef, max.toString());
            Object min = min(legs, "accumulatorAfter");
            String aft = "ACCUMULATOR_" + c+"_VALUE_AFT";
            m.put(aft, min.toString());
        }
        m.put("ACCUMULATOR_ID_USED", k);
        return m;
    }

    public Map<String, String> handleAccumulators() {
        Map<String, String> m = new LinkedHashMap<>();
        Map<String, Accumulator> changes = new HashMap<>();
        Set<Map.Entry<String, List<Map<String, Object>>>> accountEntrySet = accuAccounts.entrySet();
        for(Map.Entry<String, List<Map<String, Object>>> accumulatorEntry : accountEntrySet){
            String accumulatorId = accumulatorEntry.getKey();
            Accumulator accumulator = new Accumulator();
            Long accumulatorChange = 0L, accumulatorBefore = null, accumulatorAfter = 0L;
            for(Map<String,Object> accAccountEntry : accumulatorEntry.getValue()){
                if(accumulatorBefore == null){
                    if(accAccountEntry.get("accumulatorBefore") != null){
                        String val = accAccountEntry.get("accumulatorBefore").toString();
                        accumulatorBefore = Long.parseLong(val);
                    }
                }
                if(accAccountEntry.get("accumulatorChange") != null){
                    String val = accAccountEntry.get("accumulatorChange").toString();
                    accumulatorChange += Long.parseLong(val);
                }
                if(accAccountEntry.get("accumulatorAfter") != null){
                    String val = accAccountEntry.get("accumulatorAfter").toString();
                    accumulatorAfter = Long.parseLong(val);
                }
            }
            accumulator.setAccumulatorId(accumulatorId);
            accumulator.setAccumulatorBefore(accumulatorBefore);
            accumulator.setAccumulatorChange(accumulatorChange);
            accumulator.setAccumulatorAfter(accumulatorAfter);
            changes.put(accumulatorId, accumulator);
        }
        if(!changes.isEmpty()) {
            LinkedHashMap<String, Accumulator> topAccumulators = sortByComparator(changes, false);
            List<Accumulator> accumulatorList = new LinkedList<>(topAccumulators.values());

            long totalChange = 0, totalAfter = 0, totalBefore = 0;
            String aChange = "", aAfter = "", aBefore = "";
            List<String> acclrs = new ArrayList<>();
            List<String> thresholds = new ArrayList<>();
            long totalThChange = 0, totalThAfter = 0, totalThBefore = 0;
            String aThChange = "", aThAfter = "", aThBefore = "";
            for(int index = 1; index <= maxNumberOfAccumulators; index++){
                if(accumulatorList.size() < index) break;
                Accumulator accumulator = accumulatorList.get(index-1);
                if(accumulator != null) {
                    String accumulatorIDLabel = "ACCUMULATOR_" + index;
                    m.put(accumulatorIDLabel + "_VALUE", String.valueOf(accumulator.getAccumulatorChange()));
                    m.put(accumulatorIDLabel + "_BEF", String.valueOf(accumulator.getAccumulatorBefore()));
                    m.put(accumulatorIDLabel + "_AFT", String.valueOf(accumulator.getAccumulatorAfter()));

                    /* per new changes Accumulators vs Thresholds */
                    long c = accumulator.getAccumulatorChange() == null?0:accumulator.getAccumulatorChange();
                    if (c < 0){
                        if (StringUtils.isNotEmpty(aThChange)) aThChange += "|";
                        thresholds.add(accumulator.getAccumulatorId());
                        aThChange += String.format("%d", c);
                        totalThChange += c;

                        if (StringUtils.isNotEmpty(aThBefore)) aThBefore += "|";
                        long b = accumulator.getAccumulatorBefore() == null ? 0 : accumulator.getAccumulatorBefore();
                        aThBefore += String.format("%d", b);
                        totalThBefore += b;

                        if (StringUtils.isNotEmpty(aThAfter)) aThAfter += "|";
                        long a = accumulator.getAccumulatorAfter() == null ? 0 : accumulator.getAccumulatorAfter();
                        aThAfter += String.format("%d", a);
                        totalThAfter += a;
                    }else {
                        acclrs.add(accumulator.getAccumulatorId());

                        if (StringUtils.isNotEmpty(aChange)) aChange += "|";
                        aChange += String.format("%d", c);
                        totalChange += c;

                        if (StringUtils.isNotEmpty(aBefore)) aBefore += "|";
                        long b = accumulator.getAccumulatorBefore() == null ? 0 : accumulator.getAccumulatorBefore();
                        aBefore += String.format("%d", b);
                        totalBefore += b;

                        if (StringUtils.isNotEmpty(aAfter)) aAfter += "|";
                        long a = accumulator.getAccumulatorAfter() == null ? 0 : accumulator.getAccumulatorAfter();
                        aAfter += String.format("%d", a);
                        totalAfter += a;
                    }
                }
            }

            m.put("ACCUMULATOR_ID_USED", StringUtils.join(acclrs,"|"));
            if (!acclrs.isEmpty()) {
                m.put("ACCUMULATOR_USAGE", String.format("%d", totalChange) + "|" + aChange);
                m.put("ACCUMULATOR_BEFORE_EVENT", String.format("%d", totalBefore) + "|" + aBefore);
                m.put("ACCUMULATOR_AFTER_EVENT", String.format("%d", totalAfter) + "|" + aAfter);
            }

            m.put("COUNTER_ID_USED", StringUtils.join(thresholds,"|"));
            if (!thresholds.isEmpty()) {
                m.put("COUNTER_USAGE", String.format("%d", totalThChange) + "|" + aThChange);
                m.put("COUNTER_BEFORE_EVENT", String.format("%d", totalThBefore) + "|" + aThBefore);
                m.put("COUNTER_AFTER_EVENT", String.format("%d", totalThAfter) + "|" + aThAfter);
            }
        }
        return m;
    }

    /*
    * Sort All the Accumulators based upon accumulatorChange
    * @param unsortCollection input collection
    * @param order can be true or false. If true it signifies ASC, else DSC
    * */
    private LinkedHashMap<String, Accumulator> sortByComparator(Map<String, Accumulator> unsortCollection, final boolean order) {

        List<Map.Entry<String, Accumulator>> list = new LinkedList<>(unsortCollection.entrySet());
        Collections.sort(list, (o1, o2) -> {
            if (order) return o1.getValue().getAccumulatorChange().compareTo(o2.getValue().getAccumulatorChange());
            else return o2.getValue().getAccumulatorChange().compareTo(o1.getValue().getAccumulatorChange());
        });

        // Maintaining insertion order with the help of LinkedList
        LinkedHashMap<String, Accumulator> sortedMap = new LinkedHashMap<>();
        for (Map.Entry<String, Accumulator> entry : list) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }

    Object min(List<Map<String, Object>> listOfMap, String key) {
        List<Object> l = listOfMap.stream().map(e -> e.get(key))
                .sorted().collect(Collectors.toList());
        if (l.size() > 0) {
            return l.get(0);
        }
        return "";
    }

    Object max(List<Map<String, Object>> listOfMap, String key) {
        List<Object> l = listOfMap.stream().map(e -> e.get(key))
                .sorted().collect(Collectors.toList());
        if (l.size() > 0) {
            return l.get(l.size() - 1);
        }
        return "";
    }

    private static class Accumulator{
        private String accumulatorId;
        private Long accumulatorBefore, accumulatorAfter, accumulatorChange;

        public String getAccumulatorId() {
            return accumulatorId;
        }

        public void setAccumulatorId(String accumulatorId) {
            this.accumulatorId = accumulatorId;
        }

        public Long getAccumulatorBefore() {
            return accumulatorBefore;
        }

        public void setAccumulatorBefore(Long accumulatorBefore) {
            this.accumulatorBefore = accumulatorBefore;
        }

        public Long getAccumulatorAfter() {
            return accumulatorAfter;
        }

        public void setAccumulatorAfter(Long accumulatorAfter) {
            this.accumulatorAfter = accumulatorAfter;
        }

        public Long getAccumulatorChange() {
            return accumulatorChange;
        }

        public void setAccumulatorChange(Long accumulatorChange) {
            this.accumulatorChange = accumulatorChange;
        }
    }
}
