package com.ifchange.spark.bi.tob;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.bi.bean.cv.Algorithms;
import com.ifchange.spark.bi.bean.cv.IndustryMapping;
import com.ifchange.spark.bi.bean.cv.ResumeExtra;
import com.ifchange.spark.bi.bean.tob.*;
import com.ifchange.spark.util.MyString;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * idw_tob_resume 增量数据处理
 * base_industry
 */
public class ProcessIdwTobResumeBaseIndustry {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessIdwTobResumeBaseIndustry.class);

    private static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String tobResumeDetailPath = args[2];
        String tobAlgoPath = args[3];
        String industryPath = args[4];

        Calendar instance = Calendar.getInstance();
        instance.add(Calendar.DATE, -1);
        Date time = instance.getTime();
        final String date = DTF.format(time.toInstant().atZone(ZoneId.systemDefault()).toLocalDate());

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse");

        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        //加载industry_mapping /user/hive/warehouse/edw_dim_dimension.db/industry_mapping/part-00000-c0e3973b-671c-4549-a23d-6289f643bc6d-c000.txt
        Dataset<IndustryMapping> industryMappingDs = sparkSession.read()
            .textFile(industryPath)
            .map((MapFunction<String, IndustryMapping>) s -> {
                IndustryMapping industryMapping = new IndustryMapping();
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                Integer industryId = Integer.parseInt(split[0]);
                Integer pindustryId = Integer.parseInt(split[1]);
                Integer depth = Integer.parseInt(split[2]);
                industryMapping.setId(industryId);
                industryMapping.setPindustry_id(pindustryId);
                industryMapping.setDepth(depth);
                return industryMapping;
            }, Encoders.bean(IndustryMapping.class));

        Dataset<ResumeExtra> resumeExtraDataset = sparkSession.read()
            .textFile(tobResumeDetailPath)
            .filter((FilterFunction<String>) s -> {
                boolean flag = false;
                if (StringUtils.isNoneBlank(s)) {
                    String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                    if (split.length == 4) {
                        String id = split[0];
                        if (StringUtils.isNoneBlank(id) && StringUtils.isNumeric(id)) {
                            flag = true;
                        }
                    }
                }
                return flag;
            }).map((MapFunction<String, ResumeExtra>) s -> {
                ResumeExtra resumeExtra = new ResumeExtra();
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                try {
                    Long id = Long.parseLong(split[0]);
                    resumeExtra.setId(id);
                } catch (Exception e) {
                    LOG.error("parse extra data error:{}", e.getMessage());
                    e.printStackTrace();
                }
                String compress = split[1];
                String updatedAt = split[2];
                String createdAt = split[3];
                resumeExtra.setCompress(compress);
                resumeExtra.setCreatedAt(createdAt);
                resumeExtra.setUpdatedAt(updatedAt);
                return resumeExtra;
            }, Encoders.bean(ResumeExtra.class));

        Dataset<Algorithms> algorithmsDataset = sparkSession.read()
            .textFile(tobAlgoPath)
            .filter((FilterFunction<String>) s -> {
                boolean flag = false;
                if (StringUtils.isNoneBlank(s)) {
                    String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                    if (split.length == 4) {
                        String id = split[0];
                        if (StringUtils.isNoneBlank(id) && StringUtils.isNumeric(id)) {
                            flag = true;
                        }
                    }
                }
                return flag;
            }).map((MapFunction<String, Algorithms>) s -> {
                Algorithms algorithms = new Algorithms();
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                try {
                    Long id = Long.parseLong(split[0]);
                    algorithms.setId(id);
                    String json = split[1];
                    if (StringUtils.isNoneBlank(json)) {
                        JSONObject jsonObject = null;
                        try {
                            jsonObject = JSONObject.parseObject(json);
                        } catch (Exception e) {
                            LOG.info("id:{} algorithms parse json error:{}", id, e.getMessage());
                        }
                        if (null != jsonObject) {
                            String cvTradeStr = null != jsonObject.get("cv_trade") ?
                                jsonObject.getString("cv_trade") : "";
                            String cvTrade = StringUtils.isNoneBlank(cvTradeStr) ?
                                new String(MyString.gzipUncompress(Base64.getDecoder().decode(cvTradeStr))) : "";
                            algorithms.setCvTrade(cvTrade);
                        }
                    }
                } catch (Exception e) {
                    LOG.error("parse algorithms data error:{}", e.getMessage());
                }

                String updatedAt = split[2];
                String createdAt = split[3];
                algorithms.setCreatedAt(createdAt);
                algorithms.setUpdatedAt(updatedAt);
                return algorithms;
            }, Encoders.bean(Algorithms.class));

        //resume_extra left join algorithms
        Dataset<IdwTobBaseIndustryTempIncre> baseIndustryTempDs = resumeExtraDataset
            .join(algorithmsDataset, resumeExtraDataset.col("id").equalTo(algorithmsDataset.col("id")), "left")
            .select(resumeExtraDataset.col("id"),
                resumeExtraDataset.col("compress"),
                resumeExtraDataset.col("updatedAt"),
                algorithmsDataset.col("cvTrade"))
            .flatMap((FlatMapFunction<Row, IdwTobBaseIndustryTempIncre>) row -> {
                List<IdwTobBaseIndustryTempIncre> baseIndustryTemps = new ArrayList<>();
                Long resumeId = row.getLong(0);
                String compressStr = null != row.get(1) ? row.getString(1) : "";
                String updatedAt = null != row.get(2) ? row.getString(2) : "";
                String cvTrade = null != row.get(3) ? row.getString(3) : "";
                if (StringUtils.isNoneBlank(compressStr)) {
                    String compress = new String(MyString.gzipUncompress(Base64.getDecoder().decode(compressStr)));
                    JSONObject jsonObject = null;
                    if (StringUtils.isNoneBlank(compress)) {
                        try {
                            jsonObject = JSONObject.parseObject(compress);
                        } catch (Exception e) {
                            LOG.info("id:{} compress cannot parse to json,msg:{}", resumeId, e.getMessage());
                        }
                    }
                    if (null != jsonObject) {
                        if (StringUtils.isNoneBlank(cvTrade)) {
                            JSONArray array = null;
                            try {
                                array = JSONObject.parseArray(cvTrade);
                            } catch (Exception e) {
                                LOG.info("id:{} cvTrade parse json error:{}", resumeId, e.getMessage());
                            }
                            if (null != array && array.size() > 0) {
                                for (int i = 0; i < array.size(); i++) {
                                    JSONObject tradeJson = array.getJSONObject(i);
                                    if (null != tradeJson && tradeJson.size() > 0) {
                                        String wid = tradeJson.getString("work_id");
                                        List<Integer> tradeList = null != tradeJson.get("second_trade_list") ?
                                            (List<Integer>) tradeJson.get("second_trade_list") :
                                            (List<Integer>) tradeJson.get("first_trade_list");
                                        if (null != tradeList && tradeList.size() > 0) {
                                            for (int industryId : tradeList) {
                                                IdwTobBaseIndustryTempIncre baseIndustryTemp = new IdwTobBaseIndustryTempIncre();
                                                baseIndustryTemp.setDay(date);
                                                baseIndustryTemp.setResume_id(resumeId);
                                                baseIndustryTemp.setWid(wid);
                                                baseIndustryTemp.setUpdated_at(updatedAt);
                                                baseIndustryTemp.setIndustry_id(industryId);
                                                LOG.info("id:{}->wid:{}->industry_id:{}", resumeId, wid, industryId);
                                                baseIndustryTemps.add(baseIndustryTemp);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                return baseIndustryTemps.iterator();
            }, Encoders.bean(IdwTobBaseIndustryTempIncre.class));

        baseIndustryTempDs.join(industryMappingDs,
            baseIndustryTempDs.col("industry_id").equalTo(industryMappingDs.col("id")), "left")
            .select(baseIndustryTempDs.col("resume_id"),
                baseIndustryTempDs.col("wid"),
                industryMappingDs.col("pindustry_id"),
                industryMappingDs.col("depth"),
                baseIndustryTempDs.col("updated_at"))
            .map((MapFunction<Row, IdwTobBaseIndustryIncre>) row -> {
                IdwTobBaseIndustryIncre industryIncre = new IdwTobBaseIndustryIncre();
                long resumeId = row.getLong(0);
                String wid = null != row.get(1) ? row.getString(1) : "";
                int pIndustryId = null != row.get(2) ? row.getInt(2) : 0;
                int depth = null != row.get(3) ? row.getInt(3) : 0;
                String updatedAt = null != row.get(4) ? row.getString(4) : "";
                industryIncre.setWid(wid);
                industryIncre.setDay(date);
                industryIncre.setDepth(depth);
                industryIncre.setPindustry_id(pIndustryId);
                industryIncre.setResume_id(resumeId);
                industryIncre.setUpdated_at(updatedAt);
                return industryIncre;
            }, Encoders.bean(IdwTobBaseIndustryIncre.class))
            .write().mode(SaveMode.Append).partitionBy("day")
            .saveAsTable("idw_tob_resume.base_industry_incre");
    }

}
