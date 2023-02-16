package com.gamma.skybase.build.server.etl.tx.hlr;

import java.util.LinkedHashMap;
import java.util.Optional;

public class HlrEnrichmentUtil {
    LinkedHashMap<String, Object> rec;

    private HlrEnrichmentUtil(LinkedHashMap<String, Object> record) {
        rec = record;
    }

    public static HlrEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new HlrEnrichmentUtil(record);
    }

    public String getValue(String field) {
        Object s = rec.get(field);
        if (s != null) {
            String s1 = s.toString().trim();
            if (!s1.equals("")) return s1;
        }
        return null;
    }

    public Optional<String> getNetworkAccessMode() {
        String networkAccessMode = null;
        String nam;
        nam = getValue("NAM");
        if (nam != null) {
            switch (nam) {
                case "0":
                    networkAccessMode = "PS+CS";
                    break;
                case "1":
                    networkAccessMode = "MSC-CS Mode";
                    break;
                case "2":
                    networkAccessMode = "SGSN_PC Mode";
                    break;
                default:
                    networkAccessMode = "-99";
                    break;
            }
        }
        if (networkAccessMode != null)
            return Optional.of(networkAccessMode);
        return Optional.empty();
    }

    public Optional<String> getOdbIncomingCall() {
        String odbic;
        String odbIncomingCall = null;
        odbic = getValue("ODBIC");
        if (odbic != null) {
            switch (odbic) {
                case "0":
                    odbIncomingCall = "NO_BAR";
                    break;
                case "1":
                    odbIncomingCall = "IBARRED";
                    break;
                case "2":
                    odbIncomingCall = "BARRED_WHILE_ROAMING";
                    break;
                default:
                    break;
            }
        }
        if (odbIncomingCall != null)
            return Optional.of(odbIncomingCall);
        return Optional.empty();
    }

    public Optional<String> getOdbOutgoingCall() {
        String odboc ;
        String odbOutgoingCall = null;
        odboc = getValue("ODBOC");
        if (odboc != null) {
            switch (odboc) {
                case "0":
                    odbOutgoingCall = "NO_BAR";
                    break;
                case "1":
                    odbOutgoingCall = "BARRED";
                    break;
                case "2":
                    odbOutgoingCall = "OUTGOING_INTERNATIONAL_BARRED";
                    break;
                case "3":
                    odbOutgoingCall = "OUTGOING_INTERNATIONAL_EXCEPT_TO_HOME_BARRED";
                    break;
                case "4":
                    odbOutgoingCall = "OUTGOING_BARRED_WHILE_ROAMING";
                    break;
                default:
                    odbOutgoingCall = "-99";
                    break;
            }
        }
        if (odbOutgoingCall != null)
            return Optional.of(odbOutgoingCall);
        return Optional.empty();
    }

    public Optional<String> getObdPlmn1() {
        String obdPlamn1;
        String obdPlamn11 = null;
        obdPlamn1 = getValue("ODBPLMN1");
        if (obdPlamn1 != null) {
            switch (obdPlamn1) {
                case "0":
                    obdPlamn11 = "NOBAR";
                    break;
                case "1":
                    obdPlamn11 = "BARRED";
                    break;
                default:
                    break;
            }
        }
        if (obdPlamn11 != null)
            return Optional.of(obdPlamn11);
        return Optional.empty();
    }

    public Optional<String> getObdPlmn2() {
        String obdPlamn2 ;
        String obdPlamn21 = null;
        obdPlamn2 = getValue("ODBPLMN2");
        if (obdPlamn2 != null) {
            switch (obdPlamn2) {
                case "0":
                    obdPlamn21 = "NOBAR";
                    break;
                case "1":
                    obdPlamn21 = "BARRED";
                    break;
                default:
                    break;
            }
        }
        if (obdPlamn21 != null)
            return Optional.of(obdPlamn21);
        return Optional.empty();
    }

    public Optional<String> getObdPlmn3() {
        String obdPlamn3;
        String obdPlamn31 = null;
        obdPlamn3 = getValue("ODBPLMN3");
        if (obdPlamn3 != null) {
            switch (obdPlamn3) {
                case "0":
                    obdPlamn31 = "NOBAR";
                    break;
                case "1":
                    obdPlamn31 = "BARRED";
                    break;
                default:
                    break;
            }
        }
        if (obdPlamn31 != null)
            return Optional.of(obdPlamn31);
        return Optional.empty();
    }

    public Optional<String> getObdPlmn4() {
        String obdPlamn4;
        String obdPlamn41 = null;
        obdPlamn4 = getValue("ODBPLMN4");
        if (obdPlamn4 != null) {
            switch (obdPlamn4) {
                case "0":
                    obdPlamn41 = "NOBAR";
                    break;
                case "1":
                    obdPlamn41 = "BARRED";
                    break;
                default:
                    break;
            }
        }
        if (obdPlamn41 != null)
            return Optional.of(obdPlamn41);
        return Optional.empty();
    }

    public Optional<String> getOdbRoam() {
        String odbRoam;
        String odbRoam1 = null;
        odbRoam = getValue("ODBROAM");
        if (odbRoam != null) {
            switch (odbRoam) {
                case "0":
                    odbRoam1 = "NOBAR";
                    break;
                case "1":
                    odbRoam1 = "BARRED_OUTIODE_PLMN";
                    break;
                case "2":
                    odbRoam1 = "BARRED_OUTSIDE_PLMN_COUNTRY";
                    break;
                case "3":
                    odbRoam1 = "BARRED_GPRS_OUTSIDE_PLMN_COUNTRY";
                    break;
                default:
                    odbRoam1 = "-99";
                    break;
            }
        }
        if (odbRoam1 != null)
            return Optional.of(odbRoam1);
        return Optional.empty();
    }

    public Optional<String> getOdbdectCalltransfer() {
        String odbdect;
        String odbdectCalltransfer = null;
        odbdect = getValue("ODBDECT");
        if (odbdect != null) {
            switch (odbdect) {
                case "0":
                    odbdectCalltransfer = "NOBAR";
                    break;
                case "1":
                    odbdectCalltransfer = "BARRED";
                    break;
                default:
                    odbdectCalltransfer = "-99";
                    break;
            }
        }
        if (odbdectCalltransfer != null)
            return Optional.of(odbdectCalltransfer);
        return Optional.empty();
    }

    public Optional<String> getOdbPacketOrientedService() {
        String odbpos;
        String odbPacketOrientedService = null;
        odbpos = getValue("ODBPOS");
        if (odbpos != null) {
            switch (odbpos) {
                case "0":
                    odbPacketOrientedService = "NOBAR";
                    break;
                case "1":
                    odbPacketOrientedService = "PACKET_BARRED";
                    break;
                default:
                    odbPacketOrientedService = "-99";
                    break;
            }
        }
        if (odbPacketOrientedService != null)
            return Optional.of(odbPacketOrientedService);
        return Optional.empty();
    }

    public Optional<String> getOdbPacketOrientedServiceType() {
        String odbpostype;
        String odbPacketOrientedServiceType = null;
        odbpostype = getValue("ODBPOSTYPE");
        if (odbpostype != null) {
            switch (odbpostype) {
                case "0":
                    odbPacketOrientedServiceType = "MO_NO_VALID";
                    break;
                case "1":
                    odbPacketOrientedServiceType = "ONLY MO";
                    break;
                case "2":
                    odbPacketOrientedServiceType = " ONLY NO";
                    break;
                default:
                    odbPacketOrientedServiceType = "-99";
                    break;
            }
        }
        if (odbPacketOrientedServiceType != null)
            return Optional.of(odbPacketOrientedServiceType);
        return Optional.empty();
    }

    public Optional<String> getOdbRegCallFwd() {
        String odbrcf;
        String odbRegCallFwd = null;
        odbrcf = getValue("ODBRCF");
        if (odbrcf != null) {
            switch (odbrcf) {
                case "0":
                    odbRegCallFwd = "NOBAR";
                    break;
                case "1":
                    odbRegCallFwd = "CF_BARRED";
                    break;
                case "2":
                    odbRegCallFwd = "CFW_INTL_BARRED_EXCEPT_HPLMN";
                    break;
                case "3":
                    odbRegCallFwd = "CFW_INTL_BARRED";
                    break;
                default:
                    odbRegCallFwd = "-99";
                    break;
            }
        }
        if (odbRegCallFwd != null)
            return Optional.of(odbRegCallFwd);
        return Optional.empty();
    }

    public Optional<String> getOdbPremium() {
        String odbenter;
        String odbPremium = null;
        odbenter = getValue("ODBENTER");
        if (odbenter != null) {
            switch (odbenter) {
                case "0":
                    odbPremium = "NOBAR";
                    break;
                case "1":
                    odbPremium = "PREMIUM_BARRED";
                    break;
                default:
                    odbPremium = "-99";
                    break;
            }
        }
        if (odbPremium != null)
            return Optional.of(odbPremium);
        return Optional.empty();
    }

    public Optional<String> getOdbSuppServ() {
        String odbss;
        String odbSuppServ = null;
        odbss = getValue("ODBSS");
        if (odbss != null) {
            switch (odbss) {
                case "0":
                    odbSuppServ = "NOBAR";
                    break;
                case "1":
                    odbSuppServ = "SS_BARRED";
                    break;
                default:
                    odbSuppServ = "-99";
                    break;
            }
        }
        if (odbSuppServ != null)
            return Optional.of(odbSuppServ);
        return Optional.empty();
    }

    public Optional<String> getUtrannotallowed_3G() {
        String utrannotallowed;
        String utrannotallowed_3g = null;
        utrannotallowed = getValue("UTRANNOTALLOWED");
        if (utrannotallowed != null) {
            switch (utrannotallowed) {
                case "0":
                    utrannotallowed_3g = "Access";
                    break;
                case "1":
                    utrannotallowed_3g = "NO ACCESS";
                    break;
                default:
                    utrannotallowed_3g = "-99";
                    break;
            }
        }
        if (utrannotallowed_3g != null)
            return Optional.of(utrannotallowed_3g);
        return Optional.empty();
    }

    public Optional<String> getGerannotallowed_2G() {
        String gerannotallowed;
        String gerannotallowed_2g = null;
        gerannotallowed = getValue("UTRANNOTALLOWED");
        if (gerannotallowed != null) {
            switch (gerannotallowed) {
                case "0":
                    gerannotallowed_2g = "Access";
                    break;
                case "1":
                    gerannotallowed_2g = "NO ACCESS";
                    break;
                default:
                    gerannotallowed_2g = "-99";
                    break;
            }
        }
        if (gerannotallowed_2g != null)
            return Optional.of(gerannotallowed_2g);
        return Optional.empty();
    }

    public Optional<String> getCardType() {
        String cardType1;
        String cardType = null;
        cardType1 = getValue("UTRANNOTALLOWED");
        if (cardType1 != null) {
            switch (cardType1) {
                case "0":
                    cardType = "SIM_GSM";
                    break;
                case "1":
                    cardType = "USIM_UMTS";
                    break;
                default:
                    cardType = "-99";
                    break;
            }
        }
        if (cardType != null)
            return Optional.of(cardType);
        return Optional.empty();
    }

    public Optional<String> getOcs() {
        String ocsitpl;
        String ocs = null;
        ocsitpl = getValue("OCSITPL");
        if (ocsitpl != null) {
            if (ocsitpl.equals("65535")) {
                ocs = "SERVICE_NOT_USED";
            } else {
                ocs = ocsitpl;
            }
        }
        if (ocs != null)
            return Optional.of(ocs);
        return Optional.empty();
    }

    public Optional<String> getTcs() {
        String tcsitpl;
        String tcs = null;
        tcsitpl = getValue("TCSITPL");
        if (tcsitpl != null) {
            if (tcsitpl.equals("65535")) {
                tcs = "SERVICE_NOT_USED";
            } else {
                tcs = tcsitpl;
            }
        }
        if (tcs != null)
            return Optional.of(tcs);
        return Optional.empty();
    }

    public Optional<String> getUcs() {
        String ucsitpl;
        String ucs = null;
        ucsitpl = getValue("UCSITPL");
        if (ucsitpl != null) {
            if (ucsitpl.equals("65535")) {
                ucs = "SERVICE_NOT_USED";
            } else {
                ucs = ucsitpl;
            }
        }
        if (ucs != null)
            return Optional.of(ucs);
        return Optional.empty();
    }

    public Optional<String> getSmscCamel() {
        String smscsitpl;
        String smscCamel = null;
        smscsitpl = getValue("SMSCSITPL");
        if (smscsitpl != null) {
            if (smscsitpl.equals("65535")) {
                smscCamel = "SERVICE_NOT_USED";
            } else {
                smscCamel = smscsitpl;
            }
        }
        if (smscCamel != null)
            return Optional.of(smscCamel);
        return Optional.empty();
    }

    public Optional<String> getSmsmtCamel() {
        String mtsmscsitpl;
        String smsmtCamel = null;
        mtsmscsitpl = getValue("MTSMSCSITPL");
        if (mtsmscsitpl != null) {
            if (mtsmscsitpl.equals("65535")) {
                smsmtCamel = "SERVICE_NOT_USED";
            } else {
                smsmtCamel = mtsmscsitpl;
            }
        }
        if (smsmtCamel != null)
            return Optional.of(smsmtCamel);
        return Optional.empty();
    }

    public Optional<String> getGprsCamel() {
        String gprscsitpl;
        String gprsCamel = null;
        gprscsitpl = getValue("GPRSCSITPL");
        if (gprscsitpl != null) {
            if (gprscsitpl.equals("65535")) {
                gprsCamel = "SERVICE_NOT_USED";
            } else {
                gprsCamel = gprscsitpl;
            }
        }
        if (gprsCamel != null)
            return Optional.of(gprsCamel);
        return Optional.empty();
    }

    public Optional<String> getsuplServiceCamel() {
        String sscsitpl;
        String suplServiceCamel = null;
        sscsitpl = getValue("SSCSITPL");
        if (sscsitpl != null) {
            if (sscsitpl.equals("65535")) {
                suplServiceCamel = "SERVICE_NOT_USED";
            } else {
                suplServiceCamel = sscsitpl;
            }
        }
        if (suplServiceCamel != null)
            return Optional.of(suplServiceCamel);
        return Optional.empty();
    }

    public Optional<String> getVlrinhplmn() {
        String vlrinhplmn1;
        String vlrinhplmn = null;
        vlrinhplmn1 = getValue("VLRINHPLMN");
        if (vlrinhplmn1 != null) {
            if (vlrinhplmn1.equals("0")) {
                vlrinhplmn = "VLR_NOT_HOME";
            } else {
                vlrinhplmn = "VLR_HOME";
            }
        }
        if (vlrinhplmn != null)
            return Optional.of(vlrinhplmn);
        return Optional.empty();
    }

    public Optional<String> getVlrinternational() {
        String vlrinternational1;
        String vlrinternational = null;
        vlrinternational1 = getValue("VLRINTERNATIONAL");
        if (vlrinternational1 != null) {
            if (vlrinternational1.equals("0")) {
                vlrinternational = "VLR_NOT_INTL";
            } else {
                vlrinternational = "VLR_INTL";
            }
        }
        if (vlrinternational != null)
            return Optional.of(vlrinternational);
        return Optional.empty();
    }

    public Optional<String> getSgsninhplmn() {
        String sgsninhplmn1;
        String sgsninhplmn = null;
        sgsninhplmn1 = getValue("SGSNINHPLMN");
        if (sgsninhplmn1 != null) {
            if (sgsninhplmn1.equals("0")) {
                sgsninhplmn = "SGSN_NOT_HOME";
            } else {
                sgsninhplmn = "VLR_HOME";
            }
        }
        if (sgsninhplmn != null)
            return Optional.of(sgsninhplmn);
        return Optional.empty();
    }

    public Optional<String> getSgsninternational() {
        String sgsninternational1;
        String sgsninternational = null;
        sgsninternational1 = getValue("SSCSITPL");
        if (sgsninternational1 != null) {
            if (sgsninternational1.equals("0")) {
                sgsninternational = "SGSN_NOT_INTL";
            } else {
                sgsninternational = "VLR_INTL";
            }
        }
        if (sgsninternational != null)
            return Optional.of(sgsninternational);
        return Optional.empty();
    }

    public Optional<String> getGlobCahrgingCharacter() {
        String chargeGloba;
        String globCahrgingCharacter = null;
        chargeGloba = getValue("CHARGE_GLOBA");
        if (chargeGloba != null) {
            switch (chargeGloba) {
                case "0":
                    globCahrgingCharacter = "HOT";
                    break;
                case "2":
                    globCahrgingCharacter = "FLAT";
                    break;
                case "3":
                    globCahrgingCharacter = "PREPAID";
                    break;
                case "4":
                    globCahrgingCharacter = "NORMAL";
                    break;
                default:
                    globCahrgingCharacter = "-99";
                    break;
            }
        }
        if (globCahrgingCharacter != null)
            return Optional.of(globCahrgingCharacter);
        return Optional.empty();
    }

    public Optional<String> getOptGprstplId() {
        String optGprstplId1;
        String optGprstplId = null;
        optGprstplId1 = getValue("OPT_GPRSTPL_ID");
        if (optGprstplId1 != null) {
            if (optGprstplId1.equals("65535")) {
                optGprstplId = "SERVICE_NOT_USED";
            } else {
                optGprstplId = optGprstplId1;
            }
        }
        if (optGprstplId != null)
            return Optional.of(optGprstplId);
        return Optional.empty();
    }

    public Optional<String> getOksc() {
        String oksc1;
        String oksc = null;
        oksc1 = getValue("OPT_GPRSTPL_ID");
        if (oksc1 != null) {
            if (oksc1.equals("65535")) {
                oksc = "SERVICE_NOT_USED";
            } else {
                oksc = oksc1;
            }
        }
        if (oksc != null)
            return Optional.of(oksc);
        return Optional.empty();
    }

    public Optional<String> getGeran() {
        String gannotallowed;
        String geran = null;
        gannotallowed = getValue("GANNOTALLOWED");
        if (gannotallowed != null) {
            switch (gannotallowed) {
                case "0":
                    geran = "ACCESS";
                    break;
                case "1":
                    geran = "NO ACCESS";
                    break;
                case "-1":
                    geran = "SERVICE NOT IN USE";
                    break;
                default:
                    geran = "-99";
                    break;
            }
        }
        if (geran != null)
            return Optional.of(geran);
        return Optional.empty();
    }

    public Optional<String> getIHspaE() {
        String ihspaenotallowed;
        String iHspaE = null;
        ihspaenotallowed = getValue("IHSPAENOTALLOWED");
        if (ihspaenotallowed != null) {
            switch (ihspaenotallowed) {
                case "0":
                    iHspaE = "ACCESS";
                    break;
                case "1":
                    iHspaE = "NO ACCESS";
                    break;
                case "-1":
                    iHspaE = "SERVICE NOT IN USE";
                    break;
                default:
                    iHspaE = "-99";
                    break;
            }
        }
        if (iHspaE != null)
            return Optional.of(iHspaE);
        return Optional.empty();
    }

    public Optional<String> getEutran() {
        String eutrannotallowed;
        String eutran = null;
        eutrannotallowed = getValue("EUTRANNOTALLOWED");
        if (eutrannotallowed != null) {
            switch (eutrannotallowed) {
                case "0":
                    eutran = "ACCESS";
                    break;
                case "1":
                    eutran = "NO ACCESS";
                    break;
                case "-1":
                    eutran = "SERVICE NOT IN USE";
                    break;
                default:
                    eutran = "-99";
                    break;
            }
        }
        if (eutran != null)
            return Optional.of(eutran);
        return Optional.empty();
    }

    public Optional<String> getThreeGpp() {
        String n3gppnotallowed;
        String threeGpp = null;
        n3gppnotallowed = getValue("N3GPPNOTALLOWED");
        if (n3gppnotallowed != null) {
            switch (n3gppnotallowed) {
                case "0":
                    threeGpp = "ACCESS";
                    break;
                case "1":
                    threeGpp = "NO ACCESS";
                    break;
                case "-1":
                    threeGpp = "SERVICE NOT IN USE";
                    break;
                default:
                    threeGpp = "-99";
                    break;
            }
        }
        if (threeGpp != null)
            return Optional.of(threeGpp);
        return Optional.empty();
    }

    public Optional<String> getUserCategoryM2m() {
        String usercategory;
        String userCategoryM2m = null;
        usercategory = getValue("USERCATEGORY");
        if (usercategory != null) {
            if (usercategory.equals("0")) {
                userCategoryM2m = "COMMON SUBSCRIBER";
            } else {
                userCategoryM2m = "M2M SUBSCRIBER";
            }
        }
        if (userCategoryM2m != null)
            return Optional.of(userCategoryM2m);
        return Optional.empty();
    }

    public Optional<String> getSmsOfatpl_Id() {
        String smsOfatpl_Id1;
        String smsOfatpl_Id = null;
        smsOfatpl_Id1 = getValue("SMS_OFATPL_ID");
        if (smsOfatpl_Id1 != null) {
            if (smsOfatpl_Id1.equals("65535")) {
                smsOfatpl_Id = "SERVICE_NOT_USED";
            } else {
                smsOfatpl_Id = smsOfatpl_Id1;
            }
        }
        if (smsOfatpl_Id != null)
            return Optional.of(smsOfatpl_Id);
        return Optional.empty();
    }

    public Optional<String> getSmsFtn() {
        String smsFtn1;
        String smsFtn = null;
        smsFtn1 = getValue("SMS_FTN");
        if (smsFtn1 != null) {
            if (smsFtn1.equals("65535")) {
                smsFtn = "SERVICE_NOT_USED";
            } else {
                smsFtn = smsFtn1;
            }
        }
        if (smsFtn != null)
            return Optional.of(smsFtn);
        return Optional.empty();
    }
}