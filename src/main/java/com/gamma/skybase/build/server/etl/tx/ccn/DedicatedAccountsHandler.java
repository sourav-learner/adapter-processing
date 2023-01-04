package com.gamma.skybase.build.server.etl.tx.ccn;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by abhijit on 27/8/14
 */
public class DedicatedAccountsHandler {
    private Map<String, Map<String, Long>> accountCache = new LinkedHashMap<>();
    public List<List<Map<String, Object>>> dedicatedAccounts = new ArrayList<>();

    public Map<String, String> handleDedicatedAccounts() {
        Map<String, Long> dedAcc = null;
        String dedicatedAccountID = null;
        Map<String, String> m = new LinkedHashMap<>();
        for (List<Map<String, Object>> da : dedicatedAccounts) {
            for (Map.Entry<String, Object> e : da.stream().findFirst().get().entrySet()) {
                String k = e.getKey();
                Object v = e.getValue();
                switch (k) {
                    case "dedicatedAccountID":
                        dedicatedAccountID = v.toString();
                        dedAcc = accountCache.computeIfAbsent(dedicatedAccountID, k1 -> new LinkedHashMap<>());
                        break;
                    case "accountUnitType":
                        dedAcc.put("accountUnitType", Long.parseLong(v.toString()));
                        break;
                    case "dedicatedAccountValueBefore":
                        for (LinkedHashMap<String, Object> al : (ArrayList<LinkedHashMap<String, Object>>) v) {
                            try {
                                long amount = Long.parseLong(al.get("amount").toString());
                                Long existingAmount = dedAcc.get("dedicatedAccountValueBefore");
                                if (existingAmount == null || amount < existingAmount)
                                    dedAcc.put("dedicatedAccountValueBefore", amount);
                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        }
                        break;
                    case "dedicatedAccountValueAfter":
                        for (LinkedHashMap<String, Object> al : (ArrayList<LinkedHashMap<String, Object>>) v) {
                            try {
                                long amount = Long.parseLong(al.get("amount").toString());
                                Long existingAmount = dedAcc.get("dedicatedAccountValueAfter");
                                if (existingAmount == null || amount < existingAmount)
                                    dedAcc.put("dedicatedAccountValueAfter", amount);
                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        }
                        break;
                    case "dedicatedAccountChange":
                        dedAcc = accountCache.get(dedicatedAccountID);
                        if (dedAcc == null) dedAcc = new LinkedHashMap<>();
                        for (LinkedHashMap<String, Object> al : (ArrayList<LinkedHashMap<String, Object>>) v) {
                            try {
                                long amount = Long.parseLong(al.get("amount").toString());
                                dedAcc.merge("dedicatedAccountChange", amount, (a, b) -> b + a);
                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        }
                        break;
                    case "dedicatedAccountUnitsBefore":
                    case "dedicatedAccountUnitsAfter":
                    case "dedicatedAccountUnitsChange":
                        try {
                            long units = Long.parseLong(v.toString());
                            Long existingUnits = dedAcc.get(k);
                            if (existingUnits == null || units < existingUnits) dedAcc.put(k, units);
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                        break;
                    default:
                }
            }
        }

        //m.put("DEDICATED_ACC_INFO", String.valueOf(accountCache.size()));

        String dedicatedAccountId = "";
        String dedicatedAccUsages = "";
        double dedicatedAccUsagesTotal = 0;
        double dedicatedBalBeforeEventTotal = 0, dedicatedBalAfterEventTotal = 0;
        String dedicatedBalBeforeEvent = "", dedicatedBalAfterEvent = "";

        int i = 1;
        String dai = "DEDICATED_ACC_ID_", davb = "DEDICATED_ACCVAL_BEFORE_", dava = "DEDICATED_ACCVAL_AFTER_";

        /* Loop for DA Usage */
        for (Map.Entry<String, Map<String, Long>> acc : accountCache.entrySet()) {
            String k = acc.getKey();
            Map<String, Long> value = acc.getValue();
            if (value.get("accountUnitType") != 1) continue;

            if (dedicatedAccountId.equals("")) dedicatedAccountId = k;
            else dedicatedAccountId = dedicatedAccountId + "|" + k;
            m.put(dai + i, k);

            Long dedicatedAccountValueBefore = value.get("dedicatedAccountValueBefore");
            String t = "0", t1 = "0", t2 = null;
            if (dedicatedAccountValueBefore != null) {
                double before = (double)dedicatedAccountValueBefore / Math.pow(10, 6);
                dedicatedBalBeforeEventTotal = dedicatedBalBeforeEventTotal + before;
                t = String.format("%.6f", before);
                if (StringUtils.isNotEmpty(dedicatedBalBeforeEvent)){
                    dedicatedBalBeforeEvent = dedicatedBalBeforeEvent + "|";
                }
                dedicatedBalBeforeEvent = dedicatedBalBeforeEvent + t;
            }

            m.put(davb + i, String.valueOf(dedicatedAccountValueBefore) == null ? "" : String.valueOf(t));

            Long dedicatedAccountValueAfter = value.get("dedicatedAccountValueAfter");
            if (dedicatedAccountValueAfter != null) {
                double after = dedicatedAccountValueAfter / Math.pow(10, 6);
                dedicatedBalAfterEventTotal = dedicatedBalAfterEventTotal + after;
                t1 = String.format("%.6f", after);

                if (StringUtils.isNotEmpty(dedicatedBalAfterEvent)){
                    dedicatedBalAfterEvent = dedicatedBalAfterEvent + "|";
                }
                dedicatedBalAfterEvent = dedicatedBalAfterEvent + t1;
            }
            m.put(dava + i, String.valueOf(dedicatedAccountValueAfter) == null ? "" : String.valueOf(t1));

            Long dedicatedAccountChange = value.get("dedicatedAccountChange");
            if (dedicatedAccountChange != null) {
                double usage = dedicatedAccountChange / Math.pow(10, 6);
                dedicatedAccUsagesTotal = dedicatedAccUsagesTotal + usage;
                t2 = String.format("%.6f", usage);
                if (StringUtils.isNotEmpty(dedicatedAccUsages)){
                    dedicatedAccUsages = dedicatedAccUsages + '|';
                }
                dedicatedAccUsages = dedicatedAccUsages + t2;
            }

            i++;
        }
        m.put("DEDICATED_ACC_INFO", String.valueOf(i-1));
        m.put("DEDICATED_ACCOUNT_ID", dedicatedAccountId);
        if (dedicatedAccUsagesTotal != 0){
            dedicatedAccUsages = String.format("%.6f", dedicatedAccUsagesTotal) + "|" + dedicatedAccUsages;
            m.put("DEDICATED_ACC_USAGE", dedicatedAccUsages);
        }

        if (StringUtils.isNotEmpty(dedicatedBalBeforeEvent)){
            dedicatedBalBeforeEvent = String.format("%.6f", dedicatedBalBeforeEventTotal) + "|" + dedicatedBalBeforeEvent;
            m.put("DEDICATED_BAL_BEFORE_EVENT", dedicatedBalBeforeEvent);
        }
        if (StringUtils.isNotEmpty(dedicatedBalAfterEvent)){
            dedicatedBalAfterEvent = String.format("%.6f", dedicatedBalAfterEventTotal) + "|" + dedicatedBalAfterEvent;
            m.put("DEDICATED_BAL_AFTER_EVENT", dedicatedBalAfterEvent);
        }

        /* Loop for DA Units */
        String daAccountUnitId = "";
        String daAccUnitChange = "";
        long daAccUnitsTotal = 0;
        long daUnitsBeforeEventTotal = 0, daUnitsAfterEventTotal = 0;
        String daUnitsBeforeEvent = "", daUnitsAfterEvent = "";

        for (Map.Entry<String, Map<String, Long>> acc : accountCache.entrySet()) {
            String k = acc.getKey();
            Map<String, Long> value = acc.getValue();
            if (value.get("accountUnitType") == 1) continue;

            if (daAccountUnitId.equals("")) daAccountUnitId = k;
            else daAccountUnitId = daAccountUnitId + "|" + k;

            Long daUnitsBefore = value.get("dedicatedAccountUnitsBefore");
            String t = "0", t1 = "0", t2 = null;
            if (daUnitsBefore != null) {
                daUnitsBeforeEventTotal = daUnitsBeforeEventTotal + daUnitsBefore;
                t = String.format("%d", daUnitsBefore);
                if (StringUtils.isNotEmpty(daUnitsBeforeEvent)){
                    daUnitsBeforeEvent = daUnitsBeforeEvent + "|";
                }
                daUnitsBeforeEvent = daUnitsBeforeEvent + t;
            }

            Long daUnitsAfter = value.get("dedicatedAccountUnitsAfter");
            if (daUnitsAfter != null) {
                daUnitsAfterEventTotal = daUnitsAfterEventTotal + daUnitsAfter;
                t1 = String.format("%d", daUnitsAfter);

                if (StringUtils.isNotEmpty(daUnitsAfterEvent)){
                    daUnitsAfterEvent = daUnitsAfterEvent + "|";
                }
                daUnitsAfterEvent = daUnitsAfterEvent + t1;
            }

            Long daUnitsChange = value.get("dedicatedAccountUnitsChange");
            if (daUnitsChange != null) {
                daAccUnitsTotal = daAccUnitsTotal + daUnitsChange;
                t2 = String.format("%d", daUnitsChange);
                if (StringUtils.isNotEmpty(daAccUnitChange)){
                    daAccUnitChange = daAccUnitChange + '|';
                }
                daAccUnitChange = daAccUnitChange + t2;
            }
        }
        m.put("DEDICATED_ACC_UNIT_ID", daAccountUnitId);
        if (daAccUnitsTotal != 0){
            daAccUnitChange = String.format("%d", daAccUnitsTotal) + "|" + daAccUnitChange;
            m.put("DEDICATED_ACC_UNIT_USAGE", daAccUnitChange);
        }

        if (daUnitsBeforeEventTotal != 0){
            daUnitsBeforeEvent = String.format("%d", daUnitsBeforeEventTotal) + "|" + daUnitsBeforeEvent;
            m.put("DEDICATED_ACC_UNIT_BEFORE_EVENT", daUnitsBeforeEvent);
        }
        if (daUnitsAfterEventTotal != 0){
            daUnitsAfterEvent = String.format("%d", daUnitsAfterEventTotal) + "|" + daUnitsAfterEvent;
            m.put("DEDICATED_ACC_UNIT_AFTER_EVENT", daUnitsAfterEvent);
        }

        return m;
    }

    Object min(List<LinkedHashMap<String, Object>> listOfMap, String key) {
        List<Object> l = listOfMap.stream()
                .filter(e -> e.containsKey(key))
                .map(e -> e.get(key).toString().trim())
                .sorted().collect(Collectors.toList());
        if (l.size() > 0) {
            return l.get(0);
        }
        return null;
    }

    Object minimum(List<Map<String, Object>> listOfMap, String key) {
        List<Object> l = listOfMap.stream()
                .filter(e -> e.containsKey(key))
                .map(e -> e.get(key).toString().trim())
                .sorted().collect(Collectors.toList());
        if (l.size() > 0) {
            return l.get(0);
        }
        return "";
    }

    Object maximum(List<Map<String, Object>> listOfMap, String key) {
        List<Object> l = listOfMap.stream()
                .filter(e -> e.containsKey(key))
                .map(e -> e.get(key).toString().trim())
                .sorted().collect(Collectors.toList());
        if (l.size() > 0) {
            return l.get(l.size() - 1);
        }
        return "";
    }

    Object max(ArrayList<LinkedHashMap<String, Object>> listOfMap, String key) {
        List<Object> l = listOfMap.stream()
                .map(e -> e.get(key).toString().trim())
                .sorted().collect(Collectors.toList());
        if (l.size() > 0) {
            return l.get(l.size() - 1);
        }
        return "";
    }


}
