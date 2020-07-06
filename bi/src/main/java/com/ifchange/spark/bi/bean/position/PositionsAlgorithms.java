package com.ifchange.spark.bi.bean.position;

import java.io.Serializable;

/**
 * jd_tags:职能标签
 * jd_corporations:JD偏好公司识别
 * jd_original_corporations:发布JD的公司识别
 * jd_real_corporations:JD招聘公司的公司识别（JD可能是猎头发布，这里为JD招聘真实的公司）
 * jd_trade:公司招聘偏好标签，包括学校、行业、公司
 */
public class PositionsAlgorithms implements Serializable {

    private long id;
    private String jdFunctions;
    private String jdSchools;
    private String jdCorporations;
    private String jdOriginalCorporations;
    private String jdFeatures;
    private String jdTags;
    private String jdTrades;
    private String jdTitles;
    private String jdEntities;
    private String jdAddress;
    private String jdOther;
    private String jdRealCorporations;
    private String jdComment;
    private String jdNerSkill;
    private String humanTags;
    private String isDeleted;
    private String createdAt;
    private String updatedAt;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getJdFunctions() {
        return jdFunctions;
    }

    public void setJdFunctions(String jdFunctions) {
        this.jdFunctions = jdFunctions;
    }

    public String getJdSchools() {
        return jdSchools;
    }

    public void setJdSchools(String jdSchools) {
        this.jdSchools = jdSchools;
    }

    public String getJdCorporations() {
        return jdCorporations;
    }

    public void setJdCorporations(String jdCorporations) {
        this.jdCorporations = jdCorporations;
    }

    public String getJdOriginalCorporations() {
        return jdOriginalCorporations;
    }

    public void setJdOriginalCorporations(String jdOriginalCorporations) {
        this.jdOriginalCorporations = jdOriginalCorporations;
    }

    public String getJdFeatures() {
        return jdFeatures;
    }

    public void setJdFeatures(String jdFeatures) {
        this.jdFeatures = jdFeatures;
    }

    public String getJdTags() {
        return jdTags;
    }

    public void setJdTags(String jdTags) {
        this.jdTags = jdTags;
    }

    public String getJdTrades() {
        return jdTrades;
    }

    public void setJdTrades(String jdTrades) {
        this.jdTrades = jdTrades;
    }

    public String getJdTitles() {
        return jdTitles;
    }

    public void setJdTitles(String jdTitles) {
        this.jdTitles = jdTitles;
    }

    public String getJdEntities() {
        return jdEntities;
    }

    public void setJdEntities(String jdEntities) {
        this.jdEntities = jdEntities;
    }

    public String getJdAddress() {
        return jdAddress;
    }

    public void setJdAddress(String jdAddress) {
        this.jdAddress = jdAddress;
    }

    public String getJdOther() {
        return jdOther;
    }

    public void setJdOther(String jdOther) {
        this.jdOther = jdOther;
    }

    public String getJdRealCorporations() {
        return jdRealCorporations;
    }

    public void setJdRealCorporations(String jdRealCorporations) {
        this.jdRealCorporations = jdRealCorporations;
    }

    public String getJdComment() {
        return jdComment;
    }

    public void setJdComment(String jdComment) {
        this.jdComment = jdComment;
    }

    public String getHumanTags() {
        return humanTags;
    }

    public void setHumanTags(String humanTags) {
        this.humanTags = humanTags;
    }

    public String getIsDeleted() {
        return isDeleted;
    }

    public void setIsDeleted(String isDeleted) {
        this.isDeleted = isDeleted;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public String getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(String updatedAt) {
        this.updatedAt = updatedAt;
    }

    public String getJdNerSkill() {
        return jdNerSkill;
    }

    public void setJdNerSkill(String jdNerSkill) {
        this.jdNerSkill = jdNerSkill;
    }
}
