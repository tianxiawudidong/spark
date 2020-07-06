package com.ifchange.spark.bi.bean.cv;

import java.io.Serializable;

public class BaseWorkSalary implements Serializable {

    private Long resume_id;

    private String wid;

    private Integer sort_id;

    private Double basic_salary;

    private Double basic_salary_from;

    private Double basic_salary_to;

    private Double annual_salary;

    private Double annual_salary_from;

    private Double annual_salary_to;

    private Double expect_salary_from;

    private Double expect_salary_to;

    private Double expect_annual_salary;

    private Double expect_annual_salary_from;

    private Double expect_annual_salary_to;

    private String updated_at;

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

    public Double getBasic_salary() {
        return basic_salary;
    }

    public void setBasic_salary(Double basic_salary) {
        this.basic_salary = basic_salary;
    }

    public Double getBasic_salary_from() {
        return basic_salary_from;
    }

    public void setBasic_salary_from(Double basic_salary_from) {
        this.basic_salary_from = basic_salary_from;
    }

    public Double getBasic_salary_to() {
        return basic_salary_to;
    }

    public void setBasic_salary_to(Double basic_salary_to) {
        this.basic_salary_to = basic_salary_to;
    }

    public Double getAnnual_salary() {
        return annual_salary;
    }

    public void setAnnual_salary(Double annual_salary) {
        this.annual_salary = annual_salary;
    }

    public Double getAnnual_salary_from() {
        return annual_salary_from;
    }

    public void setAnnual_salary_from(Double annual_salary_from) {
        this.annual_salary_from = annual_salary_from;
    }

    public Double getAnnual_salary_to() {
        return annual_salary_to;
    }

    public void setAnnual_salary_to(Double annual_salary_to) {
        this.annual_salary_to = annual_salary_to;
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

    public Double getExpect_annual_salary() {
        return expect_annual_salary;
    }

    public void setExpect_annual_salary(Double expect_annual_salary) {
        this.expect_annual_salary = expect_annual_salary;
    }

    public Double getExpect_annual_salary_from() {
        return expect_annual_salary_from;
    }

    public void setExpect_annual_salary_from(Double expect_annual_salary_from) {
        this.expect_annual_salary_from = expect_annual_salary_from;
    }

    public Double getExpect_annual_salary_to() {
        return expect_annual_salary_to;
    }

    public void setExpect_annual_salary_to(Double expect_annual_salary_to) {
        this.expect_annual_salary_to = expect_annual_salary_to;
    }

    public String getUpdated_at() {
        return updated_at;
    }

    public void setUpdated_at(String updated_at) {
        this.updated_at = updated_at;
    }
}
