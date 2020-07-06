package com.ifchange.spark.algorithms;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.algorithms.http.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * jd call algorithms util
 */
public class JdAlgorithmsTagUtil {

    private static final Logger LOG = LoggerFactory.getLogger(JdAlgorithmsTagUtil.class);

//    jd_schools                cv_education_service_online                         ok
//    jd_features               jd_feature_svr_online_new_format                    ok
//    jd_tags                   tag_predict                                         ok
//    jd_trades                 jd_bi
//    jd_entities               cv_entity_new_format                                ok
//    jd_real_corporations      corp_tag                                            ok
//    jd_corporations           corp_tag                                            ok
//    jd_original_corporations  corp_tag                                            ok
//    jd_ner_skill              ner_for_batch


    private static String callJdSchool(String positionId, List<String> schoolNames) throws Exception {
        try {
            JdSchoolHttp jdSchoolHttp = new JdSchoolHttp(positionId, schoolNames);
            return jdSchoolHttp.start(jdSchoolHttp);
        } catch (Exception e) {
            throw new Exception(String.format("id:%s call jd_schools error,msg:%s", positionId, e.getMessage()));
        }
    }

    private static String callJdFeature(String positionId, String corporationName, String editedAt, String requirement,
                                        String corporationId, String userId, String description, String name) throws Exception {
        try {
            JdFeatureHttp jdFeatureHttp = new JdFeatureHttp(positionId, corporationName, editedAt, requirement, corporationId, userId, description, name);
            return jdFeatureHttp.start(jdFeatureHttp);
        } catch (Exception e) {
            throw new Exception(String.format("id:%s call jd_feature error,msg:%s", positionId, e.getMessage()));
        }
    }

    public static String callJdTag(String positionId, String requirement, String name, String description) throws Exception {
        try {
            String result = "";
            int bundle = 0;
            CvTitleHttp cvTitleHttp = new CvTitleHttp(positionId, name);
            String titleResult = cvTitleHttp.start(cvTitleHttp);
            if (StringUtils.isNoneBlank(titleResult)) {
                bundle = getTitleLevelFromHttp(titleResult);
            }
            JdTagHttp jdTagHttp = new JdTagHttp(positionId, requirement, name, description);
            String jdTagResult = jdTagHttp.start(jdTagHttp);
            if (StringUtils.isNoneBlank(jdTagResult)) {
                JSONObject json = JSONObject.parseObject(jdTagResult);
                JSONArray array = new JSONArray();
                JSONObject refZhiJiJson = new JSONObject();
                refZhiJiJson.put("bundle", bundle);
                array.add(refZhiJiJson);
                json.put("ref_zhiji", array);
                result = json.toString();
            }
//            LOG.info("position_id:{},jd_tags:{}", positionId, result);
            return result;
        } catch (Exception e) {
            throw new Exception(String.format("id:%s call jd_tags error,msg:%s", positionId, e.getMessage()));
        }
    }

    public static String callJdSkill(String positionId, String requirement, String name, String description) throws Exception {
        try {
            String jdSkillResult = "";
            JdSkillHttp jdSkillHttp = new JdSkillHttp(positionId, requirement, name, description);
            String result = jdSkillHttp.start(jdSkillHttp);
            if (StringUtils.isNoneBlank(result)) {
                JSONArray array = JSONArray.parseArray(result);
                if (null != array && array.size() > 0) {
                    JSONObject jsonObject = array.getJSONObject(0);
                    jdSkillResult = jsonObject.toString();
                }
            }
//            LOG.info("position_id:{},jd_ner_skill:{}", positionId, jdSkillResult);
            return jdSkillResult;
        } catch (Exception e) {
            throw new Exception(String.format("id:%s,call jd_ner_skill error,msg:%s", positionId, e.getMessage()));
        }
    }

    private static String callJdEntity(String positionId, String description, String requirement, String name, String userId) throws Exception {
        try {
            JdEntityHttp jdEntityHttp = new JdEntityHttp(positionId, description, requirement, name, userId);
            return jdEntityHttp.start(jdEntityHttp);
        } catch (Exception e) {
            throw new Exception(String.format("id:%s call jd_entity error,msg:%s", positionId, e.getMessage()));
        }
    }

    private static String callJdRealCorp(String positionId, String realCorporationName) throws Exception {
        try {
            JdRealCorporationsHttp jdRealCorporationsHttp = new JdRealCorporationsHttp(positionId, realCorporationName);
            String result = jdRealCorporationsHttp.start(jdRealCorporationsHttp);
            LOG.info("position_id:{},jd_real_corporations:{}", positionId, result);
            return result;
        } catch (Exception e) {
            throw new Exception(String.format("id:%s,call jd_real_corporations error,msg:%s", positionId, e.getMessage()));
        }
    }

    private static String callJdCorporations(String positionId, List<String> corporations) throws Exception {
        try {
            JdCorporationsHttp jdCorporationsHttp = new JdCorporationsHttp(positionId, corporations);
            return jdCorporationsHttp.start(jdCorporationsHttp);
        } catch (Exception e) {
            throw new Exception(String.format("id:%s,call jd_corporations error,msg:%s", positionId, e.getMessage()));
        }
    }

    public static String callJdOriginalCorporation(String positionId, String corporationName) throws Exception {
        try {
            JdOriginalCorporationsHttp jdOriginalCorporationsHttp = new JdOriginalCorporationsHttp(positionId, corporationName);
//            String result = jdOriginalCorporationsHttp.start(jdOriginalCorporationsHttp);
//            LOG.info("position_id:{},jd_original_corporations:{}", positionId, result);
//            return result;
            return jdOriginalCorporationsHttp.start(jdOriginalCorporationsHttp);
        } catch (Exception e) {
            throw new Exception(String.format("id:%s,call jd_original_corporations error,msg:%s", positionId, e.getMessage()));
        }
    }

    public static String callJdTrades(String positionId, String userId, String corpId,
                                      String jdTags, Map<String, Object> tagFlag, String jdCorporations,
                                      String jdSchools, String industries,
                                      String desc, String name, String requirement,
                                      String corpName, String address) throws Exception {
        //call tob_internal_account
        try {
//            String tobInternalAccountResult = "";
//            try {
//                TobInternalAccountHttp tobInternalAccountHttp = new TobInternalAccountHttp(userId);
//                tobInternalAccountResult = tobInternalAccountHttp.start(tobInternalAccountHttp);
//            } catch (Exception e) {
//                LOG.error("user_id:{} call tob_internal_account error:{}", userId, e.getMessage());
//            }
//            int industryId = 0;
//            String address = "";
//            if (StringUtils.isNoneBlank(tobInternalAccountResult)) {
//                JSONObject tobJson = JSONObject.parseObject(tobInternalAccountResult);
//                if (null != tobJson) {
//                    address = tobJson.getString("address");
//                    JSONArray industryArray = tobJson.getJSONArray("industry");
//                    if (null != industryArray && industryArray.size() > 0) {
//                        JSONObject industry = industryArray.getJSONObject(0);
//                        if (null != industry) {
//                            industryId = industry.getInteger("tid");
//                        }
//                    }
//                }
//            }

            int tagId = 0;
            List<Long> tagLv4Ids = new ArrayList<>();

            if (StringUtils.isNoneBlank(jdTags)) {
                JSONObject json = JSONObject.parseObject(jdTags);
                if (null != json) {
                    JSONObject refZhinengMultiJSON = json.getJSONObject("ref_zhineng_multi");
                    if (null != refZhinengMultiJSON) {
                        JSONArray mustArray = refZhinengMultiJSON.getJSONArray("must");
                        if (null != mustArray && mustArray.size() > 0) {
                            JSONObject must = mustArray.getJSONObject(0);
                            tagId = must.getInteger("function_id");
                        }

                        JSONArray jsonArray = refZhinengMultiJSON.getJSONArray("should");
                        if (null != jsonArray && jsonArray.size() > 0) {
                            for (int i = 0; i < jsonArray.size(); i++) {
                                JSONObject shouldJson = jsonArray.getJSONObject(i);
                                if (null != shouldJson && shouldJson.size() > 0) {
                                    Long functionId = shouldJson.getLong("function_id");
                                    tagLv4Ids.add(functionId);
                                }
                            }
                        }

                    }
                }
            }
            JdTradesHttp jdTradesHttp = new JdTradesHttp(positionId, requirement, desc,
                tagFlag, tagId, corpId, address, tagLv4Ids, industries,
                jdCorporations, userId, name, corpName, jdSchools);
            return jdTradesHttp.start(jdTradesHttp);
        } catch (Exception e) {
            throw new Exception(String.format("id:%s,call jd_bi error,msg:%s", positionId, e.getMessage()));
        }
    }

    private static int getTitleLevelFromHttp(String str) {
        int level = 0;
        try {
            if (StringUtils.isNoneBlank(str)) {
                JSONObject jsonObject = JSONObject.parseObject(str);
                if (null != jsonObject) {
                    JSONObject response = jsonObject.getJSONObject("response");
                    if (null != response) {
                        JSONObject results = response.getJSONObject("results");
                        if (null != results) {
                            JSONObject titles = results.getJSONObject("titles");
                            if (null != titles) {
                                JSONObject detail = titles.getJSONObject("1");
                                if (null != detail) {
                                    level = detail.getInteger("level");
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.info("parse title http result:{} error:{}", str, e.getMessage());
        }
        return level;
    }

    public static JSONObject flushAllJdAlgoForJc(JSONObject jdJson) {
        String positionId = jdJson.getString("id");
        String schools = jdJson.getString("schools");
        List<String> schoolNames = new ArrayList<>();
        if (StringUtils.isNoneBlank(schools)) {
            try {
                JSONArray array = JSONArray.parseArray(schools);
                if (null != array && array.size() > 0) {
                    for (int i = 0; i < array.size(); i++) {
                        String schoolName = array.getString(i);
                        if (StringUtils.isNoneBlank(schoolName)) {
                            schoolNames.add(schoolName);
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("id:{} schools:{} is not a array", positionId, schools);
            }
        }
        //1、jd_schools
        String jdSchool = "";
        try {
            jdSchool = callJdSchool(positionId, schoolNames);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        jdJson.put("jd_schools", jdSchool);

        //2、jd_features
        String corporationName = jdJson.getString("corporation_name");
        String editedAt = jdJson.getString("edited_at");
        String requirement = jdJson.getString("requirement");
        String corporationId = jdJson.getString("corporation_id");
        String description = jdJson.getString("description");
        String name = jdJson.getString("name");
        String userId = jdJson.getString("user_id");
        String address = jdJson.getString("address");
        String jdFeature = "";
        try {
            jdFeature = callJdFeature(positionId, corporationName, editedAt,
                requirement, corporationId, userId, description, name);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        jdJson.put("jd_features", jdFeature);

        //3、jd_tags
        String jdTag = "";
        try {
            jdTag = callJdTag(positionId, requirement, name, description);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        jdJson.put("jd_tags", jdTag);

        //4、jd_entities
        String jdEntity = "";
        try {
            jdEntity = callJdEntity(positionId, description, requirement, name, userId);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        jdJson.put("jd_entities", jdEntity);

        //5、jd_real_corporations
        String realCorporationName = jdJson.getString("real_corporation_name");
        String jdRealCorp = "";
        try {
            jdRealCorp = callJdRealCorp(positionId, realCorporationName);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        jdJson.put("jd_real_corporations", jdRealCorp);

        //6、jd_corporations
        List<String> corporationsList = new ArrayList<>();
        String corporations = jdJson.getString("corporations");
        if (StringUtils.isNoneBlank(corporations)) {
            try {
                JSONArray array = JSONArray.parseArray(corporations);
                if (null != array && array.size() > 0) {
                    for (int i = 0; i < array.size(); i++) {
                        String corpName = array.getString(i);
                        if (StringUtils.isNoneBlank(corpName)) {
                            corporationsList.add(corpName);
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("id:{} corporations:{} is not a array", positionId, schools);
            }
        }
        String jdCorporations = "";
        try {
            jdCorporations = callJdCorporations(positionId, corporationsList);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        jdJson.put("jd_corporations", jdCorporations);

        //7、jd_original_corporations
        String jdOriginalCorporation = "";
        try {
            jdOriginalCorporation = callJdOriginalCorporation(positionId, corporationName);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        jdJson.put("jd_original_corporations", jdOriginalCorporation);

        //8、jd_trades jd_bi
        String tagFlag = jdJson.getString("tag_flag");
        Map<String, Object> tagFlagMap = new HashMap<>();
        if (StringUtils.isNoneBlank(tagFlag)) {
            try {
                tagFlagMap = JSONObject.parseObject(tagFlag);
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error("id:{} tag_flag:{} parse to {} error:{}", positionId, tagFlag, e.getMessage());
            }
        }
        String industries = jdJson.getString("industries");
        String jdTrades = "";
        try {
            jdTrades = callJdTrades(positionId, userId, corporationId,
                jdTag, tagFlagMap, jdCorporations,
                jdSchool, industries,
                description, name, requirement,
                corporationName, address);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        jdJson.put("jd_trades", jdTrades);

        //9、jd_ner_skill
        String jdSkill = "";
        try {
            jdSkill = callJdSkill(positionId, requirement, name, description);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        jdJson.put("jd_ner_skill", jdSkill);

        return jdJson;
    }


}
