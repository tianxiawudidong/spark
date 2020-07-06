package com.ifchange.spark.bi.bean.tob;

import java.io.Serializable;

public class IdwTobBaseCommon implements Serializable {

    private long resume_id;

    private String name;

    private int gender;

    private String birth;

    private int marital;

    private int account_province;

    private int account_city;

    private int address_province;

    private int address;

    private int current_status;

    private int management_experience;

    private String resume_updated_at;

    private String updated_at;

    private int degree;

    private int work_experience;

    private double quality;

    private double expect_salary_from;

    private double expect_salary_to;

    private String expect_position_name;

    public long getResume_id() {
        return resume_id;
    }

    public void setResume_id(long resume_id) {
        this.resume_id = resume_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getGender() {
        return gender;
    }

    public void setGender(int gender) {
        this.gender = gender;
    }

    public String getBirth() {
        return birth;
    }

    public void setBirth(String birth) {
        this.birth = birth;
    }

    public int getMarital() {
        return marital;
    }

    public void setMarital(int marital) {
        this.marital = marital;
    }

    public int getAccount_province() {
        return account_province;
    }

    public void setAccount_province(int account_province) {
        this.account_province = account_province;
    }

    public int getAccount_city() {
        return account_city;
    }

    public void setAccount_city(int account_city) {
        this.account_city = account_city;
    }

    public int getAddress_province() {
        return address_province;
    }

    public void setAddress_province(int address_province) {
        this.address_province = address_province;
    }

    public int getAddress() {
        return address;
    }

    public void setAddress(int address) {
        this.address = address;
    }

    public int getCurrent_status() {
        return current_status;
    }

    public void setCurrent_status(int current_status) {
        this.current_status = current_status;
    }

    public int getManagement_experience() {
        return management_experience;
    }

    public void setManagement_experience(int management_experience) {
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

    public int getDegree() {
        return degree;
    }

    public void setDegree(int degree) {
        this.degree = degree;
    }

    public int getWork_experience() {
        return work_experience;
    }

    public void setWork_experience(int work_experience) {
        this.work_experience = work_experience;
    }

    public double getQuality() {
        return quality;
    }

    public void setQuality(double quality) {
        this.quality = quality;
    }

    public double getExpect_salary_from() {
        return expect_salary_from;
    }

    public void setExpect_salary_from(double expect_salary_from) {
        this.expect_salary_from = expect_salary_from;
    }

    public double getExpect_salary_to() {
        return expect_salary_to;
    }

    public void setExpect_salary_to(double expect_salary_to) {
        this.expect_salary_to = expect_salary_to;
    }

    public String getExpect_position_name() {
        return expect_position_name;
    }

    public void setExpect_position_name(String expect_position_name) {
        this.expect_position_name = expect_position_name;
    }
}
