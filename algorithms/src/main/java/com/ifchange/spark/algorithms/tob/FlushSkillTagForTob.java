package com.ifchange.spark.algorithms.tob;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.algorithms.CvAlgorithmsTagUtil;
import com.ifchange.spark.algorithms.http.CvSkillHttp;
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
import java.util.*;


public class FlushSkillTagForTob {

    private static final Logger LOG = LoggerFactory.getLogger(FlushSkillTagForTob.class);

    private static final String HOST1 = "192.168.9.134";

    private static final String HOST2 = "192.168.9.167";

    private static final String USERNAME = "dataapp_user";

    private static final String PASSWORD = "Q6pXGo8cP3";

    private static final String PASSWORD2 = "lz4PWZDK/aMWz";

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
        LongAccumulator resumeContentAccumulator = sparkSession.sparkContext().longAccumulator("resume_content_accumulator");
        LongAccumulator resumeWithNoWorkAccumulator = sparkSession.sparkContext().longAccumulator("resume_no_work_accumulator");

        Dataset<String> result = sparkSession.read().textFile(path).repartition(partition)
            .mapPartitions((MapPartitionsFunction<String, String>) iterator -> {
                CvSkillHttp.init(5);
                Mysql mysql134 = new Mysql(USERNAME, PASSWORD, "tob_resume_pool_0", HOST1, PORT);
                Mysql mysql167 = new Mysql(USERNAME, PASSWORD2, "tob_resume_pool_4", HOST2, PORT);

                List<String> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    String line = iterator.next();
                    if (StringUtils.isNoneBlank(line)) {
                        String id = line.trim();
                        list.add(id);
                        //tob_resume_pool_
                        String dbName = ResumeUtil.getTobDBName(id);
                        String tableName = ResumeUtil.getTobDetailTableName(id);
                        String algorithmsDBName = ResumeUtil.getTobAlgorithmsDBName(id);
                        String algorithmsTableName = ResumeUtil.getTobAlgorithmsTableName(id);
                        String updateTableName = ResumeUtil.getTobResumesUpdateTableName(id);
                        int index = Integer.parseInt(dbName.split("_")[3]);
                        String sql = "select * from `" + dbName + "`." + tableName + " where tob_resume_id=" + id;
                        LOG.info(sql);
                        try {
                            String resumeContent = "";
                            try {
                                resumeContent = index < 4 ? mysql134.getTobResumeDetailContent(sql)
                                    : mysql167.getTobResumeDetailContent(sql);
                            } catch (Exception e) {
                                LOG.info("---------------------------------------------------");
                                LOG.info("id:{} get resume content error:{}", id, e.getMessage());
                                LOG.info("---------------------------------------------------");
                                resumeContentAccumulator.add(1L);
                            }
                            if (StringUtils.isNoneBlank(resumeContent)) {
                                Map<String, Object> compress = null;
                                try {
                                    compress = JSONObject.parseObject(resumeContent);
                                } catch (Exception e) {
                                    LOG.info("id:{} content parse to json error:{}", id, e.getMessage());
                                }

                                //查询cv_tag -->should
                                String cvTagSql = "select column_get(data,'cv_tag' as char) as cv_tag from "
                                    + algorithmsDBName + ".`" + algorithmsTableName + "` where tob_resume_id=" + id;
                                LOG.info(cvTagSql);
                                String cvTag = index < 4 ? mysql134.getAlgorithmsCvTagForTob(cvTagSql)
                                    : mysql167.getAlgorithmsCvTagForTob(cvTagSql);
                                Map<String, String> functionMap = new HashMap<>();
                                if (StringUtils.isNoneBlank(cvTag) && !StringUtils.equals("\"\"", cvTag)) {
                                    Map<String, Object> cvTagMap = null;
                                    try {
                                        cvTagMap = JSONObject.parseObject(cvTag);
                                    } catch (Exception e) {
                                        LOG.info("id:{} cv_tag:{} cannot parse json", id, cvTag);
                                    }
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
                                }

                                if (null != compress) {
                                    String skillTag = null;
                                    try {
                                        skillTag = CvAlgorithmsTagUtil.callSkillTagHttp(id, compress, functionMap);
                                    } catch (Exception e) {
                                        LOG.info("id:{} call skill_tag error:{}", id, e.getMessage());
                                    }
                                    if (StringUtils.isNoneBlank(skillTag)) {
//                                        LOG.info("id:{} skill_tag:{}", id, skillTag);
                                        String value = MyString.toUnicode(skillTag, false, false);
                                        String encoder = new String(Base64.getEncoder().encode(MyString.gzipCompress(value, "UTF-8")));
                                        String param = "'" + "skill_tag" + "'" + "," + "'" + encoder + "'";
                                        String updatedAt = dtf.format(LocalDateTime.now());
                                        try {
                                            String sql2 = "update `" + algorithmsDBName + "`." + algorithmsTableName + " set data=column_add(data," + param + ")," +
                                                "updated_at='" + updatedAt + "'  where tob_resume_id=" + id;
//                                            LOG.info(sql2);
                                            int flag = index < 4 ? mysql134.executeUpdate(sql2)
                                                : mysql167.executeUpdate(sql2);
                                            LOG.info("id:{} update skill_tag flag:{}", id, flag);
                                            if (flag != 0) {
                                                resumeAccumulator.add(1L);
                                            }
                                        } catch (Exception e) {
                                            LOG.info("id:{} save algorithms error:{}", id, e.getMessage());
                                        }

                                        if (isUpdate == 1) {
                                            try {
                                                String sql3 = "update `" + algorithmsDBName + "`." + updateTableName + " set updated_at='" + updatedAt + "'  where tob_resume_id=" + id;
                                                LOG.info(sql3);
                                                int flag2 = index < 4 ? mysql134.executeUpdate(sql3)
                                                    : mysql167.executeUpdate(sql3);
                                                LOG.info("id:{} update resumes_update flag:{}", id, flag2);
                                            } catch (Exception e) {
                                                LOG.info("id:{} update resumes_update error:{}", id, e.getMessage());
                                            }
                                        }

                                    } else {
                                        LOG.info("id:{} skill_tag result is empty", id);
                                        resumeWithNoWorkAccumulator.add(1L);
                                    }
                                }
                            } else {
                                LOG.info("id:{} resume content is empty", id);
                                resumeContentAccumulator.add(1L);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
                mysql134.close();
                mysql167.close();
                return list.iterator();
            }, Encoders.STRING());

        long count = result.count();
        LOG.info("process number:{}", count);
        LOG.info("*************************************************************************");
        LOG.info("resume accumulator value:{}", resumeAccumulator.value());
        LOG.info("resume with no work accumulator value:{}", resumeWithNoWorkAccumulator.value());
        LOG.info("resume content empty value:{}", resumeContentAccumulator.value());
        LOG.info("*************************************************************************");

    }

}
