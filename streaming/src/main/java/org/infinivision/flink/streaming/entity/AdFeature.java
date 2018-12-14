package org.infinivision.flink.streaming.entity;

public class AdFeature {
    private String aid;
    private String advertiserId;
    private String campaignId;
    private String creativeId;
    private int creativeSize;
    private String adCategoryId;
    private String productId;
    private String productType;

    public AdFeature() {
    }

    public AdFeature(String aid, String advertiserId, String campaignId, String creativeId,
                     int creativeSize, String adCategoryId, String productId, String productType) {
        this.aid = aid;
        this.advertiserId = advertiserId;
        this.campaignId = campaignId;
        this.creativeId = creativeId;
        this.creativeSize = creativeSize;
        this.adCategoryId = adCategoryId;
        this.productId = productId;
        this.productType = productType;
    }

    public String getAid() {
        return aid;
    }

    public void setAid(String aid) {
        this.aid = aid;
    }

    public String getAdvertiserId() {
        return advertiserId;
    }

    public void setAdvertiserId(String advertiserId) {
        this.advertiserId = advertiserId;
    }

    public String getCampaignId() {
        return campaignId;
    }

    public void setCampaignId(String campaignId) {
        this.campaignId = campaignId;
    }

    public String getCreativeId() {
        return creativeId;
    }

    public void setCreativeId(String creativeId) {
        this.creativeId = creativeId;
    }

    public int getCreativeSize() {
        return creativeSize;
    }

    public void setCreativeSize(int creativeSize) {
        this.creativeSize = creativeSize;
    }

    public String getAdCategoryId() {
        return adCategoryId;
    }

    public void setAdCategoryId(String adCategoryId) {
        this.adCategoryId = adCategoryId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getProductType() {
        return productType;
    }

    public void setProductType(String productType) {
        this.productType = productType;
    }

    public static AdFeature fromString(String eventStr) {
        String[] split = eventStr.split(",");
        return new AdFeature(split[0], split[1], split[2], split[3],
                Integer.valueOf(split[4]), split[5],split[6], split[7]);
    }

    @Override
    public String toString() {
        return String.join(",", aid, advertiserId, campaignId, creativeId,
                Integer.toString(creativeSize), adCategoryId, productId, productType);
    }
}
