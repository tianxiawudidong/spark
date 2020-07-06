package com.ifchange.spark.bi.bean.position.incre;

import java.io.Serializable;

public class IdwPositionsIncre implements Serializable {

    private long position_id;
    private int company_id;
    private int origin_company_id;
    private int real_company_id;
    private int salary_begin;
    private int salary_end;
    private int experience_begin;
    private int experience_end;
    private int degree_id;
    private int user_id;
    private int status;
    private int number;
    private int gender;
    //职级 jd_tags.ref_zhiji.bundle
    private int level;
    private String created_at;
    private String updated_at;
    private String refreshed_at;
    private String edited_at;
    private String day;
    /**
     * update at 2020/04/17
     * idw_positions.positions 增加position_name字段
     */
    private String position_name;

    public int getOrigin_company_id() {
        return origin_company_id;
    }

    public void setOrigin_company_id(int origin_company_id) {
        this.origin_company_id = origin_company_id;
    }

    public int getReal_company_id() {
        return real_company_id;
    }

    public void setReal_company_id(int real_company_id) {
        this.real_company_id = real_company_id;
    }

    public long getPosition_id() {
        return position_id;
    }

    public void setPosition_id(long position_id) {
        this.position_id = position_id;
    }

    public int getCompany_id() {
        return company_id;
    }

    public void setCompany_id(int company_id) {
        this.company_id = company_id;
    }

    public int getSalary_begin() {
        return salary_begin;
    }

    public void setSalary_begin(int salary_begin) {
        this.salary_begin = salary_begin;
    }

    public int getSalary_end() {
        return salary_end;
    }

    public void setSalary_end(int salary_end) {
        this.salary_end = salary_end;
    }

    public int getExperience_begin() {
        return experience_begin;
    }

    public void setExperience_begin(int experience_begin) {
        this.experience_begin = experience_begin;
    }

    public int getExperience_end() {
        return experience_end;
    }

    public void setExperience_end(int experience_end) {
        this.experience_end = experience_end;
    }

    public int getDegree_id() {
        return degree_id;
    }

    public void setDegree_id(int degree_id) {
        this.degree_id = degree_id;
    }

    public int getUser_id() {
        return user_id;
    }

    public void setUser_id(int user_id) {
        this.user_id = user_id;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public int getGender() {
        return gender;
    }

    public void setGender(int gender) {
        this.gender = gender;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public String getCreated_at() {
        return created_at;
    }

    public void setCreated_at(String created_at) {
        this.created_at = created_at;
    }

    public String getUpdated_at() {
        return updated_at;
    }

    public void setUpdated_at(String updated_at) {
        this.updated_at = updated_at;
    }

    public String getRefreshed_at() {
        return refreshed_at;
    }

    public void setRefreshed_at(String refreshed_at) {
        this.refreshed_at = refreshed_at;
    }

    public String getEdited_at() {
        return edited_at;
    }

    public void setEdited_at(String edited_at) {
        this.edited_at = edited_at;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public String getPosition_name() {
        return position_name;
    }

    public void setPosition_name(String position_name) {
        this.position_name = position_name;
    }
}
