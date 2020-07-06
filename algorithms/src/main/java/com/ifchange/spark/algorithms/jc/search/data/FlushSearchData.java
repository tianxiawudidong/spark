package com.ifchange.spark.algorithms.jc.search.data;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.mysql.Mysql;
import com.ifchange.spark.util.ResumeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * search_data 增加cvJson
 */
public class FlushSearchData {

    private static final Logger LOG = LoggerFactory.getLogger(FlushSearchData.class);

    private static final String ICDC_HOST_1 = "192.168.8.134";

    private static final String ICDC_HOST_2 = "192.168.8.136";

    private static final int ICDC_PORT = 3306;

    private static final String USER_NAME = "databus_user";

    private static final String PASS_WORD = "Y,qBpk5vT@zVOwsz";


    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String path = args[2];
        int partition = Integer.parseInt(args[3]);
        String savePath = args[4];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        Dataset<String> results = sparkSession.read().textFile(path).repartition(partition)
            .mapPartitions((MapPartitionsFunction<String, String>) iterator -> {
                Mysql icdcMysql1 = new Mysql(USER_NAME, PASS_WORD, "icdc_0", ICDC_HOST_1, ICDC_PORT);
                Mysql icdcMysql2 = new Mysql(USER_NAME, PASS_WORD, "icdc_1", ICDC_HOST_2, ICDC_PORT);

                List<String> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    String str = iterator.next();
                    if (StringUtils.isNoneBlank(str)) {
                        StringBuilder sb = new StringBuilder();
                        String[] split = str.split("\t");
                        if (split.length == 36) {
                            String cid = split[10];
                            sb.append(str);
                            sb.append("\t");
                            JSONObject cvJson = new JSONObject();
                            if (!StringUtils.equals("0", cid.trim())) {
                                //组装cvJSon
                                String icdcDbName = ResumeUtil.getDBNameById2(Long.parseLong(cid.trim()));
                                String cvSql = "select rs.compress,column_json(a.data) as algorithms_data from `" + icdcDbName + "`.resumes_extras rs " +
                                    " LEFT JOIN `" + icdcDbName + "`.algorithms a on rs.id=a.id where rs.id=" + cid;
                                String mapSql = "select * from `" + icdcDbName + "`.resumes_maps rm where rm.resume_id=" + cid;
                                LOG.info(cvSql);
                                LOG.info(mapSql);
                                try {
                                    int index = Integer.parseInt(icdcDbName.split("_")[1]);
                                    String s = index % 2 == 0 ? icdcMysql1.queryCompressAndAlgorithms(cvSql)
                                        : icdcMysql2.queryCompressAndAlgorithms(cvSql);
                                    if (StringUtils.isNoneBlank(s)) {
                                        JSONObject jsonObject = JSONObject.parseObject(s);
                                        List<Map<String, String>> resumeMapsList = index % 2 == 0
                                            ? icdcMysql1.queryResumesMaps(mapSql) : icdcMysql2.queryResumesMaps(mapSql);
                                        jsonObject.put("map", resumeMapsList);
                                        cvJson.put(cid, jsonObject);
                                    }
                                } catch (Exception e) {
                                    LOG.info("query icdc sql error:{}", e.getMessage());
                                    e.printStackTrace();
                                }
                            }
                            sb.append(cvJson);
                        } else {
                            LOG.info("length is not 36,is:{}", split.length);
                        }
                        list.add(sb.toString());
                    }
                }
                icdcMysql1.close();
                icdcMysql2.close();
                return list.iterator();
            }, Encoders.STRING());

        //save text
        results.write().text(savePath);
    }
}
