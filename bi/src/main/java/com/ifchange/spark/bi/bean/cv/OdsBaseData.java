package com.ifchange.spark.bi.bean.cv;

import java.io.Serializable;
import java.util.List;

public class OdsBaseData implements Serializable {

    /**
     * base_common
     */
    private BaseCommon baseCommon;

    /**
     * base_company
     */
    private List<BaseCompany> baseCompanys;

    /**
     * base_education
     */
    private List<BaseEducation> baseEducations;

    /**
     * base_function
     */
    private List<BaseFunction> baseFunctions;

    /**
     * base_industry
     */
    private List<BaseIndustryTemp> baseIndustryTemps;

    /**
     * base_salary
     */
    private BaseSalary baseSalary;

    /**
     * base_work
     */
    private List<BaseWork> baseWorks;

    /**
     * base_work_salary
     */
    private List<BaseWorkSalary> baseWorkSalaries;

    /**
     * base_work_skill
     */
    private List<BaseWorkSkill> baseWorkSkills;


    private List<BaseLevel> baseLevels;

    private List<BaseLanguage> baseLanguages;

    private List<BaseCertificate> baseCertificates;


    public BaseCommon getBaseCommon() {
        return baseCommon;
    }

    public void setBaseCommon(BaseCommon baseCommon) {
        this.baseCommon = baseCommon;
    }

    public List<BaseCompany> getBaseCompanys() {
        return baseCompanys;
    }

    public void setBaseCompanys(List<BaseCompany> baseCompanys) {
        this.baseCompanys = baseCompanys;
    }

    public List<BaseEducation> getBaseEducations() {
        return baseEducations;
    }

    public void setBaseEducations(List<BaseEducation> baseEducations) {
        this.baseEducations = baseEducations;
    }

    public List<BaseFunction> getBaseFunctions() {
        return baseFunctions;
    }

    public void setBaseFunctions(List<BaseFunction> baseFunctions) {
        this.baseFunctions = baseFunctions;
    }

    public BaseSalary getBaseSalary() {
        return baseSalary;
    }

    public void setBaseSalary(BaseSalary baseSalary) {
        this.baseSalary = baseSalary;
    }

    public List<BaseWork> getBaseWorks() {
        return baseWorks;
    }

    public void setBaseWorks(List<BaseWork> baseWorks) {
        this.baseWorks = baseWorks;
    }

    public List<BaseWorkSalary> getBaseWorkSalaries() {
        return baseWorkSalaries;
    }

    public void setBaseWorkSalaries(List<BaseWorkSalary> baseWorkSalaries) {
        this.baseWorkSalaries = baseWorkSalaries;
    }

    public List<BaseWorkSkill> getBaseWorkSkills() {
        return baseWorkSkills;
    }

    public void setBaseWorkSkills(List<BaseWorkSkill> baseWorkSkills) {
        this.baseWorkSkills = baseWorkSkills;
    }

    public List<BaseLevel> getBaseLevels() {
        return baseLevels;
    }

    public void setBaseLevels(List<BaseLevel> baseLevels) {
        this.baseLevels = baseLevels;
    }

    public List<BaseIndustryTemp> getBaseIndustryTemps() {
        return baseIndustryTemps;
    }

    public void setBaseIndustryTemps(List<BaseIndustryTemp> baseIndustryTemps) {
        this.baseIndustryTemps = baseIndustryTemps;
    }

    public List<BaseLanguage> getBaseLanguages() {
        return baseLanguages;
    }

    public void setBaseLanguages(List<BaseLanguage> baseLanguages) {
        this.baseLanguages = baseLanguages;
    }

    public List<BaseCertificate> getBaseCertificates() {
        return baseCertificates;
    }

    public void setBaseCertificates(List<BaseCertificate> baseCertificates) {
        this.baseCertificates = baseCertificates;
    }
}
