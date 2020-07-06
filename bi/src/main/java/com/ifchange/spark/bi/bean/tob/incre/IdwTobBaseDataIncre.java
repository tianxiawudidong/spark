package com.ifchange.spark.bi.bean.tob.incre;



import com.ifchange.spark.bi.bean.tob.IdwTobBaseIndustryTempIncre;

import java.io.Serializable;
import java.util.List;

public class IdwTobBaseDataIncre implements Serializable {

    /**
     * base_common
     */
    private IdwTobBaseCommonIncre baseCommonIncre;

    /**
     * base_contact
     */
    private IdwTobBaseContactIncre baseContactIncre;


    /**
     * base_company
     */
    private List<IdwTobBaseCompanyIncre> baseCompanyIncres;

    /**
     * base_education
     */
    private List<IdwTobBaseEducationIncre> baseEducationIncres;

    /**
     * base_function
     */
    private List<IdwTobBaseFunctionIncre> baseFunctionIncres;

    /**
     * base_industry
     */
    private List<IdwTobBaseIndustryTempIncre> baseIndustryTempIncres;

    /**
     * base_salary
     */
    private IdwTobBaseSalaryIncre baseSalaryIncre;

    /**
     * base_work
     */
    private List<IdwTobBaseWorkIncre> baseWorkIncres;

    /**
     * base_work_salary
     */
    private List<IdwTobBaseWorkSalaryIncre> baseWorkSalaryIncres;


    private List<IdwTobBaseLevelIncre> baseLevelIncres;

    public List<IdwTobBaseLevelIncre> getBaseLevelIncres() {
        return baseLevelIncres;
    }

    public void setBaseLevelIncres(List<IdwTobBaseLevelIncre> baseLevelIncres) {
        this.baseLevelIncres = baseLevelIncres;
    }

    public IdwTobBaseCommonIncre getBaseCommonIncre() {
        return baseCommonIncre;
    }

    public void setBaseCommonIncre(IdwTobBaseCommonIncre baseCommonIncre) {
        this.baseCommonIncre = baseCommonIncre;
    }

    public IdwTobBaseContactIncre getBaseContactIncre() {
        return baseContactIncre;
    }

    public void setBaseContactIncre(IdwTobBaseContactIncre baseContactIncre) {
        this.baseContactIncre = baseContactIncre;
    }

    public List<IdwTobBaseCompanyIncre> getBaseCompanyIncres() {
        return baseCompanyIncres;
    }

    public void setBaseCompanyIncres(List<IdwTobBaseCompanyIncre> baseCompanyIncres) {
        this.baseCompanyIncres = baseCompanyIncres;
    }

    public List<IdwTobBaseEducationIncre> getBaseEducationIncres() {
        return baseEducationIncres;
    }

    public void setBaseEducationIncres(List<IdwTobBaseEducationIncre> baseEducationIncres) {
        this.baseEducationIncres = baseEducationIncres;
    }

    public List<IdwTobBaseFunctionIncre> getBaseFunctionIncres() {
        return baseFunctionIncres;
    }

    public void setBaseFunctionIncres(List<IdwTobBaseFunctionIncre> baseFunctionIncres) {
        this.baseFunctionIncres = baseFunctionIncres;
    }

    public List<IdwTobBaseIndustryTempIncre> getBaseIndustryTempIncres() {
        return baseIndustryTempIncres;
    }

    public void setBaseIndustryTempIncres(List<IdwTobBaseIndustryTempIncre> baseIndustryTempIncres) {
        this.baseIndustryTempIncres = baseIndustryTempIncres;
    }

    public IdwTobBaseSalaryIncre getBaseSalaryIncre() {
        return baseSalaryIncre;
    }

    public void setBaseSalaryIncre(IdwTobBaseSalaryIncre baseSalaryIncre) {
        this.baseSalaryIncre = baseSalaryIncre;
    }

    public List<IdwTobBaseWorkIncre> getBaseWorkIncres() {
        return baseWorkIncres;
    }

    public void setBaseWorkIncres(List<IdwTobBaseWorkIncre> baseWorkIncres) {
        this.baseWorkIncres = baseWorkIncres;
    }

    public List<IdwTobBaseWorkSalaryIncre> getBaseWorkSalaryIncres() {
        return baseWorkSalaryIncres;
    }

    public void setBaseWorkSalaryIncres(List<IdwTobBaseWorkSalaryIncre> baseWorkSalaryIncres) {
        this.baseWorkSalaryIncres = baseWorkSalaryIncres;
    }
}
