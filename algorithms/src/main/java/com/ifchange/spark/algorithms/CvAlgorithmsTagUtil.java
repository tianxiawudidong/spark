package com.ifchange.spark.algorithms;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.algorithms.http.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 调用算法封装
 * cv_trade,
 * cv_title,
 * cv_tag,
 * cv_entity,
 * cv_education,
 * cv_feature,
 * skill_tag,
 * cv_degree,
 * cv_workyear
 */
public class CvAlgorithmsTagUtil {

    private static final Logger LOG = LoggerFactory.getLogger(CvAlgorithmsTagUtil.class);

    /**
     * cv 根据tag 刷具体的算法
     */
    @SuppressWarnings("unchecked")
    public static Map<String, String> algorithmsTagForCv(String resumeId, Map<String, Object> compress) {
        Map<String, String> result = new HashMap<>();
//        if (StringUtils.equalsIgnoreCase(tag, "all")) {
        //1、cv_trade公司和行业识别
        String cvTrade = "";
        try {
            cvTrade = callCvTrade(resumeId, compress);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        result.put("cv_trade", cvTrade);

        //2、cv_title职级
        String cvTitle = "";
        try {
            cvTitle = callCvTitle(resumeId, compress);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        result.put("cv_title", cvTitle);

        //3、cv_tag职能/标签
        String cvTag = "";
        try {
            cvTag = callCvTag(resumeId, compress);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        result.put("cv_tag", cvTag);
        Map<String, String> functionMap = new HashMap<>();
        if (StringUtils.isNoneBlank(cvTag)) {
            Map<String, Object> maps = JSONObject.parseObject(cvTag);
            if (null != maps && maps.size() > 0) {
                for (Map.Entry<String, Object> entry : maps.entrySet()) {
                    String functionId = "";
                    String workId = entry.getKey();
                    try {
                        Map<String, Object> detail = (Map<String, Object>) entry.getValue();
                        List<String> shouldList = (List<String>) detail.get("should");
                        if (null != shouldList && shouldList.size() > 0) {
                            String should = shouldList.get(0);
                            functionId = StringUtils.splitByWholeSeparator(should, ":")[0];
                        }
                        functionMap.put(workId, functionId);
                    } catch (Exception e) {
                        functionMap.put(workId, functionId);
                    }
                }
            }
        }

        //4、cv_entity职能职级
        String cvEntity = "";
        try {
            cvEntity = callCvEntity(resumeId, compress);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        result.put("cv_entity", cvEntity);

        //5、cv_education学校&专业
        String cvEducation = "";
        try {
            cvEducation = callCvEducation(resumeId, compress);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        result.put("cv_education", cvEducation);

        //6、cv_degree
        String cvDegree = "";
        try {
            cvDegree = callCvDegree(resumeId, compress);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        result.put("cv_degree", cvDegree);

        //7、cv_feature特征提取
        String cvFeature = "";
        try {
            cvFeature = callCvFeature(resumeId, compress);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        result.put("cv_feature", cvFeature);

        //8、cv_workyear工作年限
        String cvWorkYear = "";
        try {
            cvWorkYear = callCvWorkYear(resumeId, compress);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        result.put("cv_workyear", cvWorkYear);

        //9、cv_quality简历质量 依赖cv_title、cv_tag
        String qualityResult = "";
        try {
            CvQualityHttp cvQualityForHttp = new CvQualityHttp(resumeId, cvEducation, cvDegree, cvTrade,
                cvWorkYear, cvTag, compress);
            qualityResult = cvQualityForHttp.start(cvQualityForHttp);
//            LOG.info("id:{} cv_quality:{}", resumeId, qualityResult);
        } catch (Exception e) {
            LOG.info("id:{} call cv_quality error:{}", resumeId, e.getMessage());
        }
        if (StringUtils.isNotBlank(qualityResult)) {
            //{"cvid":140660316,"score":0.15300000000000002,"_score":0.153,"score_detail":{"degreeScore":0,"corpScore":0,"workExpScore":0.6,"schoolScore":0.04}}
            try {
                JSONObject jsonObject = JSONObject.parseObject(qualityResult);
                if (null != jsonObject) {
                    String score = jsonObject.getString("score");
                    result.put("cv_quality", score);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //10、cv_language语言识别
        String cvLanguage = "";
        try {
            cvLanguage = callCvLanguage(resumeId, compress);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        result.put("cv_language", cvLanguage);

        //11、cv_certificate证书识别
        String cvCertificate = "";
        try {
            cvCertificate = callCvCertificate(resumeId, compress);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        result.put("cv_certificate", cvCertificate);

        //12、skill_tag
        String skillTagHttp = callSkillTagHttp(resumeId, compress, functionMap);
        result.put("skill_tag", skillTagHttp);

        //13、cv_current_status
        int currentStatus = 0;
        try {
            CvCurrentStatus cvCurrentStatus = new CvCurrentStatus(resumeId, compress);
            currentStatus = cvCurrentStatus.start(cvCurrentStatus);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        result.put("cv_current_status", String.valueOf(currentStatus));
        return result;
    }


    public static String callCvDegree(String resumeId, Map<String, Object> compress) throws Exception {
        try {
            String degree = "";
            CvDegreeHttp cvDegreeHttp = new CvDegreeHttp(resumeId, compress);
            String result = cvDegreeHttp.start(cvDegreeHttp);
            if (StringUtils.isNoneBlank(result)) {
                JSONObject map = JSONObject.parseObject(result);
                if (null != map) {
                    JSONObject response = map.getJSONObject("response");
                    if (null != response) {
                        JSONObject results = response.getJSONObject("results");
                        if (null != results && results.size() > 0) {
                            degree = StringUtils.isNotBlank(results.getString("degree")) ? results.getString("degree") : "0";
                        }
                    }
                }
            }
            return degree;
        } catch (Exception e) {
            LOG.info("id:{} call cv_degree error:{}", resumeId, e.getMessage());
            throw new Exception(e);
        }
    }

    public static String callCvEducation(String resumeId, Map<String, Object> compress) throws Exception {
        try {
            String eduResult = "";
            CvEducationHttp cvEducationHttp = new CvEducationHttp(resumeId, compress);
            String eduHttpResult = cvEducationHttp.start(cvEducationHttp);
            if (StringUtils.isNoneBlank(eduHttpResult)) {
                JSONObject map = JSONObject.parseObject(eduHttpResult);
                if (null != map) {
                    JSONObject response = map.getJSONObject("response");
                    if (null != response) {
                        JSONObject results = response.getJSONObject("results");
                        if (null != results) {
                            eduResult = results.getString("schools");
                        }
                    }
                }
            }
            return eduResult;
        } catch (Exception e) {
            LOG.info("id:{} call cv_education error:{}", resumeId, e.getMessage());
            throw new Exception(e);
        }
    }

    public static String callCvEntity(String resumeId, Map<String, Object> compress) throws Exception {
        try {
            String result = "";
            CvEntityHttp cvEntityHttp = new CvEntityHttp(resumeId, compress);
            String entityResult = cvEntityHttp.start(cvEntityHttp);
            if (StringUtils.isNotBlank(entityResult)) {
                JSONObject map = JSONObject.parseObject(entityResult);
                if (null != map) {
                    JSONObject response = map.getJSONObject("response");
                    if (null != response) {
                        result = response.getString("results");
                    }
                }
            }
            return result;
        } catch (Exception e) {
            LOG.info("id:{} call cv_entity error:{}", resumeId, e.getMessage());
            throw new Exception(e);
        }
    }

    public static String callCvFeature(String resumeId, Map<String, Object> compress) throws Exception {
        try {
            String result = "";
            CvFeatureHttp cvFeatureHttp = new CvFeatureHttp(resumeId, compress);
            String featureResult = cvFeatureHttp.start(cvFeatureHttp);
            if (StringUtils.isNotBlank(featureResult)) {
                JSONObject json = JSONObject.parseObject(featureResult);
                if (null != json) {
                    JSONObject response = json.getJSONObject("response");
                    if (null != response) {
                        result = response.getString("results");
                    }
                }
            }
            return result;
        } catch (Exception e) {
            LOG.info("id:{} call cv_feature error:{}", resumeId, e.getMessage());
            throw new Exception(e);
        }
    }


    private static String callCvLanguage(String resumeId, Map<String, Object> compress) throws Exception {
        try {
            String result = "";
            CvLanguageHttp cvLanguageHttp = new CvLanguageHttp(resumeId, compress);
            String langResult = cvLanguageHttp.start(cvLanguageHttp);
            if (StringUtils.isNotBlank(langResult)) {
                JSONObject json = JSONObject.parseObject(langResult);
                if (null != json) {
                    JSONObject response = json.getJSONObject("response");
                    if (null != response) {
                        result = response.getString("results");
                    }
                }
            }
            return result;
        } catch (Exception e) {
            LOG.info("id:{} call cv_language error:{}", resumeId, e.getMessage());
            throw new Exception(e);
        }
    }

    private static String callCvCertificate(String resumeId, Map<String, Object> compress) throws Exception {
        try {
            String result = "";
            CvCertificateHttp cvCertificateHttp = new CvCertificateHttp(resumeId, compress);
            String cerResult = cvCertificateHttp.start(cvCertificateHttp);
            if (StringUtils.isNotBlank(cerResult)) {
                JSONObject jsonObject = JSONObject.parseObject(cerResult);
                if (null != jsonObject && jsonObject.size() > 0) {
                    JSONObject response = jsonObject.getJSONObject("response");
                    if (null != response) {
                        result = response.getString("results");
                    }
                }
            }
            return result;
        } catch (Exception e) {
            LOG.info("id:{} call cv_certificate error:{}", resumeId, e.getMessage());
            throw new Exception(e);
        }
    }

    public static String callCvTag(String resumeId, Map<String, Object> compress) throws Exception {
        try {
            String result = "";
            CvTagHttp cvTagHttp = new CvTagHttp(resumeId, compress);
            String cvTagHttpResult = cvTagHttp.start(cvTagHttp);
            if (StringUtils.isNotBlank(cvTagHttpResult)) {
                JSONObject map = JSONObject.parseObject(cvTagHttpResult);
                if (null != map) {
                    JSONObject response = map.getJSONObject("response");
                    if (null != response) {
                        result = response.getString("results");
                    }
                }
            }
            return result;
        } catch (Exception e) {
            LOG.info("id:{} call cv_tag error:{}", resumeId, e.getMessage());
            throw new Exception(e);
        }
    }

    public static String callCvTitle(String resumeId, Map<String, Object> compress) throws Exception {
        String result = "";
        try {
            CvTitleHttp cvTitleHttp = new CvTitleHttp(resumeId, compress);
            String titleResult = cvTitleHttp.start(cvTitleHttp);
            if (StringUtils.isNotBlank(titleResult)) {
                JSONObject json = JSONObject.parseObject(titleResult);
                if (null != json) {
                    JSONObject response = json.getJSONObject("response");
                    if (null != response) {
                        JSONObject results = response.getJSONObject("results");
                        if (null != results) {
                            result = results.getString("titles");
                        }
                    }
                }
            }
            return result;
        } catch (Exception e) {
            LOG.info("id:{} call cv_title error:{}", resumeId, e.getMessage());
            throw new Exception(e);
        }
    }

    public static String callCvTrade(String resumeId, Map<String, Object> compress) throws Exception {
        try {
            String result = "";
            CropTagHttp cropTagHttp = new CropTagHttp(resumeId, compress);
            String cropTagHttpResult = cropTagHttp.start(cropTagHttp);
            if (StringUtils.isNotBlank(cropTagHttpResult)) {
                JSONObject jsonObject = JSONObject.parseObject(cropTagHttpResult);
                JSONObject responseJson = jsonObject.getJSONObject("response");
                result = responseJson.getString("result");
            }
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.info("id:{} call crop_tag error:{}", resumeId, e.getMessage());
            throw new Exception(e);
        }
    }

    public static String callCvWorkYear(String resumeId, Map<String, Object> compress) throws Exception {
        try {
            String result = "";
            CvWorkYearHttp cvWorkYearHttp = new CvWorkYearHttp(resumeId, compress);
            String workYearHttpResult = cvWorkYearHttp.start(cvWorkYearHttp);
            if (StringUtils.isNotBlank(workYearHttpResult)) {
                JSONObject json = JSONObject.parseObject(workYearHttpResult);
                if (null != json) {
                    JSONObject response = json.getJSONObject("response");
                    if (null != response) {
                        JSONObject results = response.getJSONObject("results");
                        if (null != results && results.size() > 0) {
                            result = results.getString("workyear");
                        }
                    }
                }
            }
            return result;
        } catch (Exception e) {
            LOG.info("id:{} call cv_workyear error:{}", resumeId, e.getMessage());
            throw new Exception(e);
        }
    }

    /**
     * skill_tag v2版本：输入增加function_id
     */
    public static String callSkillTagHttp(String resumeId, Map<String, Object> compress, Map<String, String> functionMap) {
        String result = "";
        try {
            long start = System.currentTimeMillis();
            CvSkillHttp cvSkillHttp = new CvSkillHttp(resumeId, compress, functionMap);
            result = cvSkillHttp.start(cvSkillHttp);
            long end = System.currentTimeMillis();
            LOG.info("id:{} call skill_tag use time:{}", resumeId, (end - start));
        } catch (Exception e) {
            LOG.info("id:{} call skill_tag error:{}", resumeId, e.getMessage());
            e.printStackTrace();
        }
        return result;
    }

    public static String callCvFeatureForAbTest(String resumeId, Map<String, Object> compress) throws Exception {
        long start = System.currentTimeMillis();
        String result;
        try {
            CvFeatureHttpForAbTest cvFeatureHttpForAbTest = new CvFeatureHttpForAbTest(resumeId, compress);
            result = cvFeatureHttpForAbTest.start(cvFeatureHttpForAbTest);
            long end = System.currentTimeMillis();
            LOG.info("id:{} call cv_feature use time:{}", resumeId, (end - start));
            return result;
        } catch (Exception e) {
            LOG.info("id:{} call cv_feature error:{}", resumeId, e.getMessage());
            throw new Exception(e);
        }

    }

    public static String callCvTagForAbTest(String resumeId, Map<String, Object> compress) throws Exception {
        long start = System.currentTimeMillis();
        String tagResult;
        try {
            CvTagHttpForAbTest cvTagHttpForAbTest = new CvTagHttpForAbTest(resumeId, compress);
            tagResult = cvTagHttpForAbTest.start(cvTagHttpForAbTest);
            long end = System.currentTimeMillis();
            LOG.info("id:{} call cv_tag use time:{}", resumeId, (end - start));
            return tagResult;
        } catch (Exception e) {
            LOG.info("id:{} call cv_tag error:{}", resumeId, e.getMessage());
            throw new Exception(e);
        }

    }


    /**
     * ats 刷 cv_trade cv_tag skill_tag
     *
     * @param resumeId resumeId
     * @param compress compress
     * @return Map<String   ,       String>
     * @throws Exception Exception
     */
    public static Map<String, String> callCvTradeTagAndSkillTag(String resumeId, Map<String, Object> compress) throws Exception {

        Map<String, String> result = new HashMap<>();

        try {
            String cvTrade = callCvTrade(resumeId, compress);
//            LOG.info("id:{},cv_trade:{}", resumeId, cvTrade);
            String cvTag = callCvTag(resumeId, compress);
//            LOG.info("id:{},cv_tag:{}", resumeId, cvTag);
            Map<String, String> functionMap = new HashMap<>();
            if (StringUtils.isNoneBlank(cvTag)) {
                Map<String, Object> cvTagMap = JSONObject.parseObject(cvTag);
                if (null != cvTagMap && cvTagMap.size() > 0) {
                    cvTagMap.forEach((k, v) -> {
                        String functionId = "";
                        try {
                            Map<String, Object> value = (Map<String, Object>) v;
                            Object shouldObj = value.get("should");
                            if (shouldObj instanceof List) {
                                List<String> shouldList = (List<String>) shouldObj;
                                if (shouldList.size() > 0) {
                                    String should = shouldList.get(0);
                                    functionId = StringUtils.splitByWholeSeparatorPreserveAllTokens(should, ":")[0];
                                }
                            }
                            functionMap.put(k, functionId);
                        } catch (Exception e) {
                            functionMap.put(k, functionId);
                        }
                    });
                }
            }
            String skillTag = callSkillTagHttp(resumeId, compress, functionMap);
//            LOG.info("id:{},skill_tag:{}", resumeId, skillTag);
            result.put("cv_tag", cvTag);
            result.put("cv_trade", cvTrade);
            result.put("skill_tag", skillTag);
        } catch (Exception e) {
            throw new Exception(String.format("call algorithms error,msg:%s", e.getMessage()));
        }
        return result;

    }


    public static String callCvDssm(String resumeId, Map<String, Object> compress) throws Exception {
        try {
            long start = System.currentTimeMillis();
            CvDssmHttp cvDssmHttp = new CvDssmHttp(resumeId, compress);
            String result = cvDssmHttp.start(cvDssmHttp);
            long end = System.currentTimeMillis();
            LOG.info("id:{} call cv_dssm use time:{}", resumeId, (end - start));
            return result;
        } catch (Exception e) {
            LOG.error("id:{} call cv_dssm error:{}", resumeId, e.getMessage());
            throw new Exception(e);
        }
    }

    public static String callCvDomain(String resumeId, Map<String, Object> compress, Map<String, String> functionMap) throws Exception {
        try {
            long start = System.currentTimeMillis();
            CvDomainHttp cvDomainHttp = new CvDomainHttp(resumeId, compress, functionMap);
            String result = cvDomainHttp.start(cvDomainHttp);
            long end = System.currentTimeMillis();
            LOG.info("id:{} call cv_domain use time:{}", resumeId, (end - start));
            return result;
        } catch (Exception e) {
            LOG.error("id:{} call cv_domain error:{}", resumeId, e.getMessage());
            throw new Exception(e);
        }
    }

    public static String callCvIndustry(String resumeId, Map<String, Object> compress, Map<String, String> functionMap,
                                        Map<String, List<Long>> corpIndustryIdsMap) throws Exception {
        try {
            long start = System.currentTimeMillis();
            CvIndustryHttp cvIndustryHttp = new CvIndustryHttp(resumeId, compress, functionMap,corpIndustryIdsMap);
            String result = cvIndustryHttp.start(cvIndustryHttp);
            long end = System.currentTimeMillis();
            LOG.info("id:{} call cv_industry use time:{}", resumeId, (end - start));
            return result;
        } catch (Exception e) {
            LOG.error("id:{} call cv_industry error:{}", resumeId, e.getMessage());
            throw new Exception(e);
        }
    }


    // cv_current_status,
    @SuppressWarnings("unchecked")
    public static Map<String, String> algorithmsTagForCvOnYueTa(String resumeId, Map<String, Object> compress) throws Exception {
        Map<String, String> result = new HashMap<>();
        //cv_trade公司和行业识别
        String cropTagHttpResult = "";
        try {
            CropTagHttp cropTagHttp = new CropTagHttp(resumeId, compress);
            cropTagHttpResult = cropTagHttp.start(cropTagHttp);
            if (StringUtils.isNotBlank(cropTagHttpResult)) {
                JSONObject jsonObject = JSONObject.parseObject(cropTagHttpResult);
                JSONObject responseJson = jsonObject.getJSONObject("response");
                String results = responseJson.getString("result");
                result.put("cv_trade", results);
            }
        } catch (Exception e) {
            LOG.info("id:{} call crop_tag error:{}", resumeId, e.getMessage());
            throw new Exception(e);
        }

        //cv_title职级
        String cvTitle = callCvTitle(resumeId, compress);
        result.put("cv_title", cvTitle);

        //cv_tag职能/标签
        String cvTagHttpResult = "";
        CvTagHttp cvTagHttp = new CvTagHttp(resumeId, compress);
        cvTagHttpResult = cvTagHttp.start(cvTagHttp);
        Map<String, String> functionMap = new HashMap<>();
        if (StringUtils.isNotBlank(cvTagHttpResult)) {
            JSONObject map = JSONObject.parseObject(cvTagHttpResult);
            if (null != map) {
                JSONObject response = map.getJSONObject("response");
                if (null != response) {
                    String results = response.getString("results");
                    result.put("cv_tag", results);
                    if (StringUtils.isNoneBlank(results)) {
                        Map<String, Object> cvTagMap = JSONObject.parseObject(results);
                        if (null != cvTagMap && cvTagMap.size() > 0) {
                            cvTagMap.forEach((k, v) -> {
                                String functionId = "";
                                try {
                                    Map<String, Object> value = (Map<String, Object>) v;
                                    Object shouldObj = value.get("should");
                                    if (shouldObj instanceof List) {
                                        List<String> shouldList = (List<String>) shouldObj;
                                        if (shouldList.size() > 0) {
                                            String should = shouldList.get(0);
                                            functionId = StringUtils.splitByWholeSeparatorPreserveAllTokens(should, ":")[0];
                                        }
                                    }
                                    functionMap.put(k, functionId);
                                } catch (Exception e) {
                                    functionMap.put(k, functionId);
                                }
                            });
                        }
                    }
                }
            }
        }

        //cv_entity职能职级
//        String cvEntity = callCvEntity(resumeId, compress);
//        result.put("cv_entity", cvEntity);

        //cv_education学校&专业
        String eduHttpResult = "";
        CvEducationHttp cvEducationHttp = new CvEducationHttp(resumeId, compress);
        eduHttpResult = cvEducationHttp.start(cvEducationHttp);
        if (StringUtils.isNoneBlank(eduHttpResult)) {
            JSONObject map = JSONObject.parseObject(eduHttpResult);
            if (null != map) {
                JSONObject response = map.getJSONObject("response");
                if (null != response) {
                    String results = response.getString("results");
                    result.put("cv_education", results);
                }
            }
        }

        //cv_degree
        String degreeResult = "";
        CvDegreeHttp cvDegreeHttp = new CvDegreeHttp(resumeId, compress);
        degreeResult = cvDegreeHttp.start(cvDegreeHttp);
        if (StringUtils.isNoneBlank(degreeResult)) {
            JSONObject map = JSONObject.parseObject(degreeResult);
            if (null != map) {
                JSONObject response = map.getJSONObject("response");
                if (null != response) {
                    JSONObject results = response.getJSONObject("results");
                    if (null != results && results.size() > 0) {
                        String degree = StringUtils.isNotBlank(results.getString("degree")) ? results.getString("degree") : "0";
                        result.put("cv_degree", degree);
                    }
                }
            }
        }

        //cv_feature特征提取
        String cvFeature = callCvFeature(resumeId, compress);
        result.put("cv_feature", cvFeature);

        //cv_workyear工作年限
        String workYearHttpResult = "";
        CvWorkYearHttp cvWorkYearHttp = new CvWorkYearHttp(resumeId, compress);
        workYearHttpResult = cvWorkYearHttp.start(cvWorkYearHttp);
        if (StringUtils.isNotBlank(workYearHttpResult)) {
            JSONObject json = JSONObject.parseObject(workYearHttpResult);
            if (null != json) {
                JSONObject response = json.getJSONObject("response");
                if (null != response) {
                    JSONObject results = response.getJSONObject("results");
                    if (null != results && results.size() > 0) {
                        String workyear = results.getString("workyear");
                        result.put("cv_workyear", workyear);
                    }
                }
            }
        }

        //cv_quality简历质量 依赖cv_title、cv_tag
//        String qualityResult;
//        try {
//            CvQualityHttp cvQualityForHttp = new CvQualityHttp(resumeId, eduHttpResult, degreeResult, cropTagHttpResult,
//                workYearHttpResult, cvTagHttpResult, compress);
//            qualityResult = cvQualityForHttp.start(cvQualityForHttp);
//        } catch (Exception e) {
//            LOG.info("id:{} call cv_quality error:{}", resumeId, e.getMessage());
//            throw new Exception(e);
//        }
//        if (StringUtils.isNotBlank(qualityResult)) {
//            result.put("cv_quality", qualityResult);
//        }
        //cv_language语言识别
        String cvLanguage = callCvLanguage(resumeId, compress);
        result.put("cv_language", cvLanguage);

        //cv_certificate证书识别
        String results = callCvCertificate(resumeId, compress);
        result.put("cv_certificate", results);

        //skill_tag
        String skillTagHttp = callSkillTagHttp(resumeId, compress, functionMap);
        result.put("skill_tag", skillTagHttp);

        //cv_current_status
        CvCurrentStatus cvCurrentStatus = new CvCurrentStatus(resumeId, compress);
        int currentStatus = cvCurrentStatus.start(cvCurrentStatus);
        result.put("cv_current_status", String.valueOf(currentStatus));

        return result;
    }


}
