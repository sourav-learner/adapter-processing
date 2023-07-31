package com.gamma.skybase.build.server.etl.decoder;

import lombok.Data;

@Data
public class ReferenceDimCbsOfferPayType {
    String offerId , offerName , primaryFlag , payMode , payModeD , offerTypeBi , segmentCategoryBi;

    // Getter methods

    public String getOfferId() {
        return offerId;
    }

    public String getOfferName() {
        return offerName;
    }

    public String getPrimaryFlag() {
        return primaryFlag;
    }

    public String getPayMode() {
        return payMode;
    }

    public String getPayModeD() {
        return payModeD;
    }

    public String getOfferTypeBi() {
        return offerTypeBi;
    }

    public String getSegmentCategoryBi() {
        return segmentCategoryBi;
    }

    // Setter methods
    public void setOfferId(String offerId) {
        this.offerId = offerId;
    }

    public void setOfferName(String offerName) {
        this.offerName = offerName;
    }

    public void setPrimaryFlag(String primaryFlag) {
        this.primaryFlag = primaryFlag;
    }

    public void setPayMode(String payMode) {
        this.payMode = payMode;
    }

    public void setPayModeD(String payModeD) {
        this.payModeD = payModeD;
    }

    public void setOfferTypeBi(String offerTypeBi) {
        this.offerTypeBi = offerTypeBi;
    }

    public void setSegmentCategoryBi(String segmentCategoryBi) {
        this.segmentCategoryBi = segmentCategoryBi;
    }
}
