package com.gamma.skybase.build.server.etl.tx.ccn;

import java.util.*;
import java.util.stream.Collectors;


/**
 * Created by abhijit on 27/8/14
 */
public class CCAccountDataHandler {
    private Map<String, ArrayList<LinkedHashMap<String, Object>>> ccAccDataList = new LinkedHashMap<>();
    ArrayList<LinkedHashMap<String, Object>> accountValues = new ArrayList<>();

    public void setCCAccData(LinkedHashMap<String, Object> param) {
        accountValues.add(param);
    }

    public Map<String, Object> handelCCAccount() {
       Map<String, Object> m = new LinkedHashMap<>();
        Object servedAccount = min(accountValues, "servedAccount");
        Object serviceClassID = min(accountValues, "serviceClassID");
        Object accountGroupID = min(accountValues, "accountGroupID");
        Object serviceOfferings = min(accountValues, "serviceOfferings");
        int accumulatedUnits = sum(accountValues, "accumulatedUnits");
        int accountUnitsDeducted = sum(accountValues, "accountUnitsDeducted");
//        Object accumulatedCosts = accountValues.get("accumulatedCost");
//        int accumulatedCost = sum(accumulatedCosts, "accumulatedCost");
        int accountValueDeducted = sum(accountValues, "accountValueDeducted");
//        m.put("accumulatedCost", accumulatedCost);
        m.put("accountValueDeducted", accumulatedUnits);
        m.put("accumulatedUnits", accumulatedUnits);
        m.put("accountUnitsDeducted", accountUnitsDeducted);
        m.put("servedAccount", servedAccount);
        m.put("serviceClassID", serviceClassID);
        m.put("accountGroupID", accountGroupID);
        m.put("serviceOfferings", serviceOfferings);
        return m;
    }

    Object min(ArrayList<LinkedHashMap<String, Object>> listOfMap, String key) {
        List<Object> l = listOfMap.stream().map(e -> e.get(key))
                .sorted().collect(Collectors.toList());
        if (l.size() > 0) {
            return l.get(0);
        }
        return "";
    }

    Object max(ArrayList<LinkedHashMap<String, Object>> listOfMap, String key) {
        List<Object> l = listOfMap.stream().map(e -> e.get(key))
                .sorted().collect(Collectors.toList());
        if (l.size() > 0) {
            return l.get(l.size() - 1);
        }
        return "";
    }
    
    Integer sum(ArrayList<LinkedHashMap<String, Object>> listOfMap, String key) {
        return listOfMap.stream()
                .filter(e -> e.containsKey(key))
                .map(e -> Integer.parseInt(e.get(key).toString()))
                .reduce(0, (a, b) -> a + b);
    }
}
