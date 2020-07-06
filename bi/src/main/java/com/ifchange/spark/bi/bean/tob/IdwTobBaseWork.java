package com.ifchange.spark.bi.bean.tob;

import java.io.Serializable;

public class IdwTobBaseWork implements Serializable {

    private long resume_id;

    private String wid;

    //从0开始
    private int sort_id;

    private String updated_at;

    private String start_time;

    private String end_time;

    private int so_far;

    private int management_experience;

    private int is_oversea;

    private String scale;

    private String industry_name;

    private String company_name;

    private String function_name;

    private String first_work_time;

    private int total_work_time;

    private String responsibilities;

    public long getResume_id() {
        return resume_id;
    }

    public void setResume_id(long resume_id) {
        this.resume_id = resume_id;
    }

    public String getWid() {
        return wid;
    }

    public void setWid(String wid) {
        this.wid = wid;
    }

    public int getSort_id() {
        return sort_id;
    }

    public void setSort_id(int sort_id) {
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

    public int getSo_far() {
        return so_far;
    }

    public void setSo_far(int so_far) {
        this.so_far = so_far;
    }

    public int getManagement_experience() {
        return management_experience;
    }

    public void setManagement_experience(int management_experience) {
        this.management_experience = management_experience;
    }

    public int getIs_oversea() {
        return is_oversea;
    }

    public void setIs_oversea(int is_oversea) {
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

    public String getFirst_work_time() {
        return first_work_time;
    }

    public void setFirst_work_time(String first_work_time) {
        this.first_work_time = first_work_time;
    }

    public int getTotal_work_time() {
        return total_work_time;
    }

    public void setTotal_work_time(int total_work_time) {
        this.total_work_time = total_work_time;
    }

    public String getResponsibilities() {
        return responsibilities;
    }

    public void setResponsibilities(String responsibilities) {
        this.responsibilities = responsibilities;
    }
}
