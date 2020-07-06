package com.ifchange.spark.bi.bean.cv.incre;


import java.io.Serializable;
import java.util.List;

public class OdsBaseDataIncre implements Serializable {

    /**
     * base_common_incre
     */
    private BaseCommonIncre baseCommon;

    /**
     * base_company_incre
     */
    private List<BaseCompanyIncre> baseCompanys;

    /**
     * base_education_incre
     */
    private List<BaseEducationIncre> baseEducations;

    /**
     * base_function_incre
     */
    private List<BaseFunctionIncre> baseFunctions;

    /**
     * base_industry_incre
     */
    private List<BaseIndustryTempIncre> baseIndustryTemps;

    /**
     * base_salary_incre
     */
    private BaseSalaryIncre baseSalary;

    /**
     * base_work_incre
     */
    private List<BaseWorkIncre> baseWorks;

    /**
     * base_work_salary_incre
     */
    private List<BaseWorkSalaryIncre> baseWorkSalaries;

    /**
     * base_work_skill_incre
     */
    private List<BaseWorkSkillIncre> baseWorkSkills;


    /**
     * base_level_incre
     */
    private List<BaseLevelIncre> baseLevels;

    /**
     * base_language_incre
     */
    private List<BaseLanguageIncre> baseLanguages;

    /**
     * base_certificate
     */
    private List<BaseCertificateIncre> baseCertificates;

    public BaseCommonIncre getBaseCommon() {
        return baseCommon;
    }

    public void setBaseCommon(BaseCommonIncre baseCommon) {
        this.baseCommon = baseCommon;
    }

    public List<BaseCompanyIncre> getBaseCompanys() {
        return baseCompanys;
    }

    public void setBaseCompanys(List<BaseCompanyIncre> baseCompanys) {
        this.baseCompanys = baseCompanys;
    }

    public List<BaseEducationIncre> getBaseEducations() {
        return baseEducations;
    }

    public void setBaseEducations(List<BaseEducationIncre> baseEducations) {
        this.baseEducations = baseEducations;
    }

    public List<BaseFunctionIncre> getBaseFunctions() {
        return baseFunctions;
    }

    public void setBaseFunctions(List<BaseFunctionIncre> baseFunctions) {
        this.baseFunctions = baseFunctions;
    }

    public List<BaseIndustryTempIncre> getBaseIndustryTemps() {
        return baseIndustryTemps;
    }

    public void setBaseIndustryTemps(List<BaseIndustryTempIncre> baseIndustryTemps) {
        this.baseIndustryTemps = baseIndustryTemps;
    }

    public BaseSalaryIncre getBaseSalary() {
        return baseSalary;
    }

    public void setBaseSalary(BaseSalaryIncre baseSalary) {
        this.baseSalary = baseSalary;
    }

    public List<BaseWorkIncre> getBaseWorks() {
        return baseWorks;
    }

    public void setBaseWorks(List<BaseWorkIncre> baseWorks) {
        this.baseWorks = baseWorks;
    }

    public List<BaseWorkSalaryIncre> getBaseWorkSalaries() {
        return baseWorkSalaries;
    }

    public void setBaseWorkSalaries(List<BaseWorkSalaryIncre> baseWorkSalaries) {
        this.baseWorkSalaries = baseWorkSalaries;
    }

    public List<BaseWorkSkillIncre> getBaseWorkSkills() {
        return baseWorkSkills;
    }

    public void setBaseWorkSkills(List<BaseWorkSkillIncre> baseWorkSkills) {
        this.baseWorkSkills = baseWorkSkills;
    }

    public List<BaseLevelIncre> getBaseLevels() {
        return baseLevels;
    }

    public void setBaseLevels(List<BaseLevelIncre> baseLevels) {
        this.baseLevels = baseLevels;
    }

    public List<BaseLanguageIncre> getBaseLanguages() {
        return baseLanguages;
    }

    public void setBaseLanguages(List<BaseLanguageIncre> baseLanguages) {
        this.baseLanguages = baseLanguages;
    }

    public List<BaseCertificateIncre> getBaseCertificates() {
        return baseCertificates;
    }

    public void setBaseCertificates(List<BaseCertificateIncre> baseCertificates) {
        this.baseCertificates = baseCertificates;
    }
}
