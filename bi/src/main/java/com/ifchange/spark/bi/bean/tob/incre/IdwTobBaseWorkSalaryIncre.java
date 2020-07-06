package com.ifchange.spark.bi.bean.tob.incre;

import java.io.Serializable;

public class IdwTobBaseWorkSalaryIncre implements Serializable {

    private long resume_id;

    private String wid;

    private int sort_id;

    private double basic_salary;

    private double basic_salary_from;

    private double basic_salary_to;

    private double annual_salary;

    private double annual_salary_from;

    private double annual_salary_to;

    private double expect_salary_from;

    private double expect_salary_to;

    private double expect_annual_salary;

    private double expect_annual_salary_from;

    private double expect_annual_salary_to;

    private String updated_at;

    private String day;

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

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

    public double getBasic_salary() {
        return basic_salary;
    }

    public void setBasic_salary(double basic_salary) {
        this.basic_salary = basic_salary;
    }

    public double getBasic_salary_from() {
        return basic_salary_from;
    }

    public void setBasic_salary_from(double basic_salary_from) {
        this.basic_salary_from = basic_salary_from;
    }

    public double getBasic_salary_to() {
        return basic_salary_to;
    }

    public void setBasic_salary_to(double basic_salary_to) {
        this.basic_salary_to = basic_salary_to;
    }

    public double getAnnual_salary() {
        return annual_salary;
    }

    public void setAnnual_salary(double annual_salary) {
        this.annual_salary = annual_salary;
    }

    public double getAnnual_salary_from() {
        return annual_salary_from;
    }

    public void setAnnual_salary_from(double annual_salary_from) {
        this.annual_salary_from = annual_salary_from;
    }

    public double getAnnual_salary_to() {
        return annual_salary_to;
    }

    public void setAnnual_salary_to(double annual_salary_to) {
        this.annual_salary_to = annual_salary_to;
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

    public double getExpect_annual_salary() {
        return expect_annual_salary;
    }

    public void setExpect_annual_salary(double expect_annual_salary) {
        this.expect_annual_salary = expect_annual_salary;
    }

    public double getExpect_annual_salary_from() {
        return expect_annual_salary_from;
    }

    public void setExpect_annual_salary_from(double expect_annual_salary_from) {
        this.expect_annual_salary_from = expect_annual_salary_from;
    }

    public double getExpect_annual_salary_to() {
        return expect_annual_salary_to;
    }

    public void setExpect_annual_salary_to(double expect_annual_salary_to) {
        this.expect_annual_salary_to = expect_annual_salary_to;
    }

    public String getUpdated_at() {
        return updated_at;
    }

    public void setUpdated_at(String updated_at) {
        this.updated_at = updated_at;
    }
}
