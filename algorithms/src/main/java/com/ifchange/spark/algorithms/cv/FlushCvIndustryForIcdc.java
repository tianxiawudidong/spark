package com.ifchange.spark.algorithms.cv;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.algorithms.CvAlgorithmsTagUtil;
import com.ifchange.spark.algorithms.http.CvIndustryHttp;
import com.ifchange.spark.mysql.Mysql;
import com.ifchange.spark.util.MyString;
import com.ifchange.spark.util.ResumeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * cv_industry
 */
public class FlushCvIndustryForIcdc {

    private static final Logger LOG = LoggerFactory.getLogger(FlushCvIndustryForIcdc.class);

    private static final String HOST1 = "192.168.8.130";

    private static final String HOST2 = "192.168.8.132";

    private static final String USERNAME = "databus_user";

    private static final String PASSWORD = "Y,qBpk5vT@zVOwsz";

    private static final int PORT = 3306;

    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {

        String master = args[0];
        String appName = args[1];
        String path = args[2];
        int partition = Integer.parseInt(args[3]);
        final int isUpdate = Integer.parseInt(args[4]);

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        LongAccumulator resumeAccumulator = sparkSession.sparkContext().longAccumulator("resume_accumulator");
        LongAccumulator resumeWithNoWorkAccumulator = sparkSession.sparkContext().longAccumulator("resume_no_work_accumulator");
        LongAccumulator resumeCompressNullAccumulator = sparkSession.sparkContext().longAccumulator("resume_compress_null_accumulator");


        Dataset<String> result = sparkSession.read().textFile(path).repartition(partition)
            .mapPartitions((MapPartitionsFunction<String, String>) iterator -> {
                CvIndustryHttp.init(5);
                Mysql mysql130 = new Mysql(USERNAME, PASSWORD, "icdc_0", HOST1, PORT);
                Mysql mysql132 = new Mysql(USERNAME, PASSWORD, "icdc_1", HOST2, PORT);

                List<String> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    String line = iterator.next();
                    if (StringUtils.isNoneBlank(line)) {
                        String id = line.trim();
                        list.add(id);
                        long resumeId = Long.parseLong(id);
                        //icdc_
                        String dbName = ResumeUtil.getDBNameById2(resumeId);
                        int index = Integer.parseInt(dbName.split("_")[1]);
                        String sql = "select * from `" + dbName + "`.resumes_extras where id=" + id;
                        LOG.info(sql);
                        try {
                            String compressStr = index % 2 == 0 ? mysql130.queryCompress(sql)
                                : mysql132.queryCompress(sql);
                            if (StringUtils.isNoneBlank(compressStr) && !StringUtils.equals("null", compressStr)) {
                                Map<String, Object> compress = null;
                                try {
                                    compress = JSONObject.parseObject(compressStr);
                                } catch (Exception e) {
                                    LOG.info("id:{} compress parse json error:{}", id, e.getMessage());
                                }
                                if (null != compress) {
                                    //查询cv_tag -->should cv_trade first_trade_list
                                    String cvTagAndTradeSql = "select column_get(data,'cv_tag' as char) as cv_tag," +
                                        " column_get(data,'cv_trade' as char) as cv_trade from "
                                        + dbName + ".`algorithms` where id=" + id;
                                    LOG.info(cvTagAndTradeSql);
                                    Map<String, String> tagAndTradeMap = index % 2 == 0 ? mysql130.getAlgorithmsCvTagAndCvTrade(cvTagAndTradeSql)
                                        : mysql132.getAlgorithmsCvTagAndCvTrade(cvTagAndTradeSql);
                                    Map<String, String> functionMap = new HashMap<>();
                                    Map<String, List<Long>> corpIndustryIdsMap = new HashMap<>();
                                    if (null != tagAndTradeMap && tagAndTradeMap.size() > 0) {
                                        String cvTag = tagAndTradeMap.get("cv_tag");
                                        if (StringUtils.isNoneBlank(cvTag)) {
                                            try {
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
                                                            e.printStackTrace();
                                                            functionMap.put(k, functionId);
                                                        }
                                                    });
                                                }
                                            } catch (Exception e) {
                                                LOG.error("id:{} process cv_tag:{} error:{}", resumeId, cvTag, e.getMessage());
                                                e.printStackTrace();
                                            }
                                        }

                                        String cvTrade = tagAndTradeMap.get("cv_trade");
                                        if (StringUtils.isNoneBlank(cvTrade)) {
                                            try {
                                                JSONArray array = JSONArray.parseArray(cvTrade);
                                                if (null != array && array.size() > 0) {
                                                    for (int i = 0; i < array.size(); i++) {
                                                        JSONObject json = array.getJSONObject(i);
                                                        String workId = json.getString("work_id");
                                                        JSONArray firstTradeList = json.getJSONArray("first_trade_list");
                                                        List<Long> cropList = new ArrayList<>();
                                                        if (null != firstTradeList && firstTradeList.size() > 0) {
                                                            for (int j = 0; j < firstTradeList.size(); j++) {
                                                                String trade = firstTradeList.getString(j);
                                                                try {
                                                                    long cropId = Long.parseLong(trade);
                                                                    cropList.add(cropId);
                                                                } catch (NumberFormatException e) {
                                                                    LOG.error("first_trade_list value:{} parse error:{}",
                                                                        trade, e.getMessage());
                                                                    e.printStackTrace();
                                                                }
                                                            }
                                                        }
                                                        corpIndustryIdsMap.put(workId, cropList);
                                                    }
                                                }
                                            } catch (Exception e) {
                                                LOG.error("id:{} process cv_trade:{} error:{}", resumeId, cvTrade, e.getMessage());
                                                e.printStackTrace();
                                            }
                                        }
                                    }

                                    String cvIndustryResult = null;
                                    try {
                                        cvIndustryResult = CvAlgorithmsTagUtil.callCvIndustry(id, compress, functionMap,corpIndustryIdsMap);
                                        LOG.info("id:{} cv_industry:{}", id, cvIndustryResult);
                                    } catch (Exception e) {
                                        LOG.info("id:{} call cv_industry error:{}", id, e.getMessage());
                                    }
                                    if (StringUtils.isNoneBlank(cvIndustryResult)) {
                                        String value = MyString.toUnicode(cvIndustryResult, false);
                                        value = value.replace("'", "\\'");
                                        String param = "'" + "cv_industry" + "'" + "," + "'" + value + "'";
                                        String updatedAt = dtf.format(LocalDateTime.now());
                                        try {
                                            String sql2 = "update `" + dbName + "`.algorithms set data=column_add(data," + param + ")," +
                                                "updated_at='" + updatedAt + "'  where id=" + id;
                                            LOG.info(sql2);
                                            int flag = index % 2 == 0 ? mysql130.executeUpdate(sql2)
                                                : mysql132.executeUpdate(sql2);
                                            LOG.info("id:{} update cv_industry flag:{}", id, flag);
                                            if (flag != 0) {
                                                resumeAccumulator.add(1L);
                                            }
                                        } catch (Exception e) {
                                            LOG.info("id:{} save algorithms error:{}", id, e.getMessage());
                                        }

                                        //update resumes_update
                                        if (isUpdate == 1) {
                                            try {
                                                String sql3 = "update `" + dbName + "`.resumes_update set updated_at='" + updatedAt + "' where id=" + id;
                                                LOG.info(sql3);
                                                int flag2 = index % 2 == 0 ? mysql130.executeUpdate(sql3)
                                                    : mysql132.executeUpdate(sql3);
                                                LOG.info("id:{} update resumes_update flag:{}", id, flag2);
                                            } catch (Exception e) {
                                                LOG.info("id:{} update resumes_update error:{}", id, e.getMessage());
                                            }
                                        }
                                    } else {
                                        LOG.info("id:{} cv_industry result is empty", id);
                                        resumeWithNoWorkAccumulator.add(1L);
                                    }

                                }
                            } else {
                                resumeCompressNullAccumulator.add(1L);
                            }


                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
                mysql130.close();
                mysql132.close();
                return list.iterator();
            }, Encoders.STRING());

        long count = result.count();
        LOG.info("process number:{}", count);

    }

}
