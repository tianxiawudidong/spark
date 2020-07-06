package com.ifchange.spark.bi.customer.portrait;

import java.io.Serializable;

public class Position  implements Serializable {
    private int id;
    private int siteId;
    private String siteUnique;
    private String sourceUnique;
    private String name;
    private int corporationId;
    private String corporationName;
    private String salary;
    private String city;
    private String publishTime;
    private String parsePublishTime;
    private String body;
    private int status;
    private int statusToh;
    private int positionId;
    private int positionTohId;

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    private String description;

    public String getImportedAt() {
        return importedAt;
    }

    public void setImportedAt(String importedAt) {
        this.importedAt = importedAt;
    }

    public String getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(String updatedAt) {
        this.updatedAt = updatedAt;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    private String importedAt;
    private int isDeleted;
    private String updatedAt;
    private String createdAt;
    private String language;
    private String education;
    private int recruitNum;
    private String welfare;
    private int salaryBegin;
    private int salaryEnd;
    private int recruitNumUp;
    private String cityIds;
    private String functionIds;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getSiteId() {
        return siteId;
    }

    public void setSiteId(int siteId) {
        this.siteId = siteId;
    }

    public String getSiteUnique() {
        return siteUnique;
    }

    public void setSiteUnique(String siteUnique) {
        this.siteUnique = siteUnique;
    }

    public String getSourceUnique() {
        return sourceUnique;
    }

    public void setSourceUnique(String sourceUnique) {
        this.sourceUnique = sourceUnique;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getCorporationId() {
        return corporationId;
    }

    public void setCorporationId(int corporationId) {
        this.corporationId = corporationId;
    }

    public String getCorporationName() {
        return corporationName;
    }

    public void setCorporationName(String corporationName) {
        this.corporationName = corporationName;
    }

    public String getSalary() {
        return salary;
    }

    public void setSalary(String salary) {
        this.salary = salary;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getPublishTime() {
        return publishTime;
    }

    public void setPublishTime(String publishTime) {
        this.publishTime = publishTime;
    }

    public String getParsePublishTime() {
        return parsePublishTime;
    }

    public void setParsePublishTime(String parsePublishTime) {
        this.parsePublishTime = parsePublishTime;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getStatusToh() {
        return statusToh;
    }

    public void setStatusToh(int statusToh) {
        this.statusToh = statusToh;
    }

    public int getPositionId() {
        return positionId;
    }

    public void setPositionId(int positionId) {
        this.positionId = positionId;
    }

    public int getPositionTohId() {
        return positionTohId;
    }

    public void setPositionTohId(int positionTohId) {
        this.positionTohId = positionTohId;
    }

    public int getIsDeleted() {
        return isDeleted;
    }

    public void setIsDeleted(int isDeleted) {
        this.isDeleted = isDeleted;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getEducation() {
        return education;
    }

    public void setEducation(String education) {
        this.education = education;
    }

    public int getRecruitNum() {
        return recruitNum;
    }

    public void setRecruitNum(int recruitNum) {
        this.recruitNum = recruitNum;
    }

    public String getWelfare() {
        return welfare;
    }

    public void setWelfare(String welfare) {
        this.welfare = welfare;
    }

    public int getSalaryBegin() {
        return salaryBegin;
    }

    public void setSalaryBegin(int salaryBegin) {
        this.salaryBegin = salaryBegin;
    }

    public int getSalaryEnd() {
        return salaryEnd;
    }

    public void setSalaryEnd(int salaryEnd) {
        this.salaryEnd = salaryEnd;
    }

    public int getRecruitNumUp() {
        return recruitNumUp;
    }

    public void setRecruitNumUp(int recruitNumUp) {
        this.recruitNumUp = recruitNumUp;
    }

    public String getCityIds() {
        return cityIds;
    }

    public void setCityIds(String cityIds) {
        this.cityIds = cityIds;
    }

    public String getFunctionIds() {
        return functionIds;
    }

    public void setFunctionIds(String functionIds) {
        this.functionIds = functionIds;
    }

    public String getFunctionNames() {
        return functionNames;
    }

    public void setFunctionNames(String functionNames) {
        this.functionNames = functionNames;
    }

    private String functionNames;
}
