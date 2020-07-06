package com.ifchange.spark.bi.bean.cv.incre;

import java.io.Serializable;

public class BaseCommonIncre implements Serializable {

    private Long resume_id;

    private String name;

    private Integer gender;

    private String birth;

    private Integer marital;

    private Integer account_province;

    private Integer account_city;

    private Integer address_province;

    private Integer address;

    private Integer current_status;

    private Integer management_experience;

    private String resume_updated_at;

    private String updated_at;

    private Integer degree;

    private Integer work_experience;

    private Double quality;

    private Double expect_salary_from;

    private Double expect_salary_to;

    private String expect_position_name;

    private String day;

    public Long getResume_id() {
        return resume_id;
    }

    public void setResume_id(Long resume_id) {
        this.resume_id = resume_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getGender() {
        return gender;
    }

    public void setGender(Integer gender) {
        this.gender = gender;
    }

    public String getBirth() {
        return birth;
    }

    public void setBirth(String birth) {
        this.birth = birth;
    }

    public Integer getMarital() {
        return marital;
    }

    public void setMarital(Integer marital) {
        this.marital = marital;
    }

    public Integer getAccount_province() {
        return account_province;
    }

    public void setAccount_province(Integer account_province) {
        this.account_province = account_province;
    }

    public Integer getAccount_city() {
        return account_city;
    }

    public void setAccount_city(Integer account_city) {
        this.account_city = account_city;
    }

    public Integer getAddress_province() {
        return address_province;
    }

    public void setAddress_province(Integer address_province) {
        this.address_province = address_province;
    }

    public Integer getAddress() {
        return address;
    }

    public void setAddress(Integer address) {
        this.address = address;
    }

    public Integer getCurrent_status() {
        return current_status;
    }

    public void setCurrent_status(Integer current_status) {
        this.current_status = current_status;
    }

    public Integer getManagement_experience() {
        return management_experience;
    }

    public void setManagement_experience(Integer management_experience) {
        this.management_experience = management_experience;
    }

    public String getResume_updated_at() {
        return resume_updated_at;
    }

    public void setResume_updated_at(String resume_updated_at) {
        this.resume_updated_at = resume_updated_at;
    }

    public String getUpdated_at() {
        return updated_at;
    }

    public void setUpdated_at(String updated_at) {
        this.updated_at = updated_at;
    }

    public Integer getDegree() {
        return degree;
    }

    public void setDegree(Integer degree) {
        this.degree = degree;
    }

    public Integer getWork_experience() {
        return work_experience;
    }

    public void setWork_experience(Integer work_experience) {
        this.work_experience = work_experience;
    }

    public Double getQuality() {
        return quality;
    }

    public void setQuality(Double quality) {
        this.quality = quality;
    }

    public Double getExpect_salary_from() {
        return expect_salary_from;
    }

    public void setExpect_salary_from(Double expect_salary_from) {
        this.expect_salary_from = expect_salary_from;
    }

    public Double getExpect_salary_to() {
        return expect_salary_to;
    }

    public void setExpect_salary_to(Double expect_salary_to) {
        this.expect_salary_to = expect_salary_to;
    }

    public String getExpect_position_name() {
        return expect_position_name;
    }

    public void setExpect_position_name(String expect_position_name) {
        this.expect_position_name = expect_position_name;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }
}
