package com.ifchange.spark.bi.bean.cv;

import java.io.Serializable;

public class BaseWork implements Serializable {

    private Long resume_id;

    private String wid;

    private Integer sort_id;

    private String updated_at;

    private String start_time;

    private String end_time;

    private Integer so_far;

    private Integer management_experience;

    private Integer is_oversea;

    private String scale;

    private String industry_name;

    private String company_name;

    private String function_name;

    private String responsibilities;

    public Long getResume_id() {
        return resume_id;
    }

    public void setResume_id(Long resume_id) {
        this.resume_id = resume_id;
    }

    public String getWid() {
        return wid;
    }

    public void setWid(String wid) {
        this.wid = wid;
    }

    public Integer getSort_id() {
        return sort_id;
    }

    public void setSort_id(Integer sort_id) {
        this.sort_id = sort_id;
    }

    public String getUpdated_at() {
        return updated_at;
    }

    public void setUpdated_at(String updated_at) {
        this.updated_at = updated_at;
    }

    public String getStart_time() {
        return start_time;
    }

    public void setStart_time(String start_time) {
        this.start_time = start_time;
    }

    public String getEnd_time() {
        return end_time;
    }

    public void setEnd_time(String end_time) {
        this.end_time = end_time;
    }

    public Integer getSo_far() {
        return so_far;
    }

    public void setSo_far(Integer so_far) {
        this.so_far = so_far;
    }

    public Integer getManagement_experience() {
        return management_experience;
    }

    public void setManagement_experience(Integer management_experience) {
        this.management_experience = management_experience;
    }

    public Integer getIs_oversea() {
        return is_oversea;
    }

    public void setIs_oversea(Integer is_oversea) {
        this.is_oversea = is_oversea;
    }

    public String getScale() {
        return scale;
    }

    public void setScale(String scale) {
        this.scale = scale;
    }

    public String getIndustry_name() {
        return industry_name;
    }

    public void setIndustry_name(String industry_name) {
        this.industry_name = industry_name;
    }

    public String getCompany_name() {
        return company_name;
    }

    public void setCompany_name(String company_name) {
        this.company_name = company_name;
    }

    public String getFunction_name() {
        return function_name;
    }

    public void setFunction_name(String function_name) {
        this.function_name = function_name;
    }

    public String getResponsibilities() {
        return responsibilities;
    }

    public void setResponsibilities(String responsibilities) {
        this.responsibilities = responsibilities;
    }
}
