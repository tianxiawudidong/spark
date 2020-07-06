package com.ifchange.spark.bi.bean.position;

import java.io.Serializable;

public class PositionsExcepts implements Serializable {

    private long id;
    private String corporations;
    private String industries;
    private String schools;
    private String skills;
    private String projects;
    private String majority;
    private int ecGender;
    private String others;
    private String tagFlag;
    private String noBackground;
    private String is985211;
    private String features;
    private int ageBegin;
    private int ageEnd;
    private String isDeleted;
    private String createdAt;
    private String updatedAt;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getCorporations() {
        return corporations;
    }

    public void setCorporations(String corporations) {
        this.corporations = corporations;
    }

    public String getIndustries() {
        return industries;
    }

    public void setIndustries(String industries) {
        this.industries = industries;
    }

    public String getSchools() {
        return schools;
    }

    public void setSchools(String schools) {
        this.schools = schools;
    }

    public String getSkills() {
        return skills;
    }

    public void setSkills(String skills) {
        this.skills = skills;
    }

    public String getProjects() {
        return projects;
    }

    public void setProjects(String projects) {
        this.projects = projects;
    }

    public String getMajority() {
        return majority;
    }

    public void setMajority(String majority) {
        this.majority = majority;
    }

    public int getEcGender() {
        return ecGender;
    }

    public void setEcGender(int ecGender) {
        this.ecGender = ecGender;
    }

    public String getOthers() {
        return others;
    }

    public void setOthers(String others) {
        this.others = others;
    }

    public String getTagFlag() {
        return tagFlag;
    }

    public void setTagFlag(String tagFlag) {
        this.tagFlag = tagFlag;
    }

    public String getNoBackground() {
        return noBackground;
    }

    public void setNoBackground(String noBackground) {
        this.noBackground = noBackground;
    }

    public String getIs985211() {
        return is985211;
    }

    public void setIs985211(String is985211) {
        this.is985211 = is985211;
    }

    public String getFeatures() {
        return features;
    }

    public void setFeatures(String features) {
        this.features = features;
    }

    public int getAgeBegin() {
        return ageBegin;
    }

    public void setAgeBegin(int ageBegin) {
        this.ageBegin = ageBegin;
    }

    public int getAgeEnd() {
        return ageEnd;
    }

    public void setAgeEnd(int ageEnd) {
        this.ageEnd = ageEnd;
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
}
