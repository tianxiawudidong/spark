package com.ifchange.spark.bi.bean.tob;


import com.ifchange.spark.bi.bean.cv.BaseIndustryTemp;

import java.io.Serializable;
import java.util.List;

public class IdwTobBaseData implements Serializable {

    /**
     * base_common
     */
    private IdwTobBaseCommon baseCommon;

    /**
     * base_contact
     */
    private IdwTobBaseContact baseContact;


    /**
     * base_company
     */
    private List<IdwTobBaseCompany> baseCompanys;

    /**
     * base_education
     */
    private List<IdwTobBaseEducation> baseEducations;

    /**
     * base_function
     */
    private List<IdwTobBaseFunction> baseFunctions;

    /**
     * base_industry
     */
    private List<BaseIndustryTemp> baseIndustryTemps;

    /**
     * base_salary
     */
    private IdwTobBaseSalary baseSalary;

    /**
     * base_work
     */
    private List<IdwTobBaseWork> baseWorks;

    /**
     * base_work_salary
     */
    private List<IdwTobBaseWorkSalary> baseWorkSalaries;


    private List<IdwTobBaseLevel> baseLevels;

    public IdwTobBaseCommon getBaseCommon() {
        return baseCommon;
    }

    public void setBaseCommon(IdwTobBaseCommon baseCommon) {
        this.baseCommon = baseCommon;
    }

    public List<IdwTobBaseCompany> getBaseCompanys() {
        return baseCompanys;
    }

    public void setBaseCompanys(List<IdwTobBaseCompany> baseCompanys) {
        this.baseCompanys = baseCompanys;
    }

    public List<IdwTobBaseEducation> getBaseEducations() {
        return baseEducations;
    }

    public void setBaseEducations(List<IdwTobBaseEducation> baseEducations) {
        this.baseEducations = baseEducations;
    }

    public List<IdwTobBaseFunction> getBaseFunctions() {
        return baseFunctions;
    }

    public void setBaseFunctions(List<IdwTobBaseFunction> baseFunctions) {
        this.baseFunctions = baseFunctions;
    }

    public List<BaseIndustryTemp> getBaseIndustryTemps() {
        return baseIndustryTemps;
    }

    public void setBaseIndustryTemps(List<BaseIndustryTemp> baseIndustryTemps) {
        this.baseIndustryTemps = baseIndustryTemps;
    }

    public IdwTobBaseSalary getBaseSalary() {
        return baseSalary;
    }

    public void setBaseSalary(IdwTobBaseSalary baseSalary) {
        this.baseSalary = baseSalary;
    }

    public List<IdwTobBaseWork> getBaseWorks() {
        return baseWorks;
    }

    public void setBaseWorks(List<IdwTobBaseWork> baseWorks) {
        this.baseWorks = baseWorks;
    }

    public List<IdwTobBaseWorkSalary> getBaseWorkSalaries() {
        return baseWorkSalaries;
    }

    public void setBaseWorkSalaries(List<IdwTobBaseWorkSalary> baseWorkSalaries) {
        this.baseWorkSalaries = baseWorkSalaries;
    }

    public List<IdwTobBaseLevel> getBaseLevels() {
        return baseLevels;
    }

    public void setBaseLevels(List<IdwTobBaseLevel> baseLevels) {
        this.baseLevels = baseLevels;
    }

    public IdwTobBaseContact getBaseContact() {
        return baseContact;
    }

    public void setBaseContact(IdwTobBaseContact baseContact) {
        this.baseContact = baseContact;
    }
}
