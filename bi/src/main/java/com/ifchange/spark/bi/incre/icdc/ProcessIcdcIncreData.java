package com.ifchange.spark.bi.incre.icdc;

import com.ifchange.spark.bi.incre.tob.ProcessTobResumeAndAlgoIncreData;
import com.ifchange.spark.mysql.Mysql;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * 处理icdc每日增量数据
 * resumes_extras
 * algorithms
 */
public class ProcessIcdcIncreData {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessTobResumeAndAlgoIncreData.class);

    private static final Logger RESUMES_EXTRAS_LOG = LoggerFactory.getLogger("RESUMES_EXTRAS_LOGGER");

    private static final Logger ALGORITHMS_LOG = LoggerFactory.getLogger("ALGORITHMS_LOGGER");

    private static final String HOST_1 = "192.168.8.134";

    private static final String HOST_2 = "192.168.8.136";

    private static final String USERNAME = "databus_user";

    private static final String PASSWORD = "Y,qBpk5vT@zVOwsz";

    private static final int PORT = 3306;

    private static final String DB_NAME_1 = "icdc_0";

    private static final String DB_NAME_2 = "icdc_1";

    private static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");


    public static void main(String[] args) throws Exception {

        long t1 = System.currentTimeMillis();
        LOG.info("init mysql");
        Mysql mysql1 = new Mysql(USERNAME, PASSWORD, DB_NAME_1, HOST_1, PORT);
        Mysql mysql2 = new Mysql(USERNAME, PASSWORD, DB_NAME_2, HOST_2, PORT);

        Calendar instance = Calendar.getInstance();
        instance.add(Calendar.DATE, -1);
        Date time = instance.getTime();
        LocalDate date = time.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        int year = date.getYear();
        int monthValue = date.getMonthValue();
        int dayOfMonth = date.getDayOfMonth();
        LocalDateTime start = LocalDateTime.of(year, monthValue, dayOfMonth, 0, 0, 1);
        LocalDateTime end = LocalDateTime.of(year, monthValue, dayOfMonth, 23, 59, 59);
        String startTime = DTF.format(start);
        String endTime = DTF.format(end);
        LOG.info("start:{}", startTime);
        LOG.info("end:{}", endTime);

        List<String> idList = new ArrayList<>();
        for (int j = 0; j < 40; j++) {
            idList.clear();
            String db = "icdc_" + j;
            //resumes_extras
            String sql = "select id,compress,cv_source,cv_trade,cv_title,cv_tag,skill_tag,personal_tag,updated_at,created_at " +
                " from `" + db + "`.resumes_extras where updated_at>='" + startTime + "' and updated_at<='" + endTime + "'";
            LOG.info(sql);
            List<Map<String, String>> list = j % 2 == 0 ? mysql1.getResumeExtra(sql) : mysql2.getResumeExtra(sql);
            if (null != list && list.size() > 0) {
                for (Map<String, String> map : list) {
                    String id = map.get("id");
                    String compress = map.get("compress");
                    String cvSource = map.get("cv_source");
                    String cvTrade = map.get("cv_trade");
                    String cvTitle = map.get("cv_title");
                    String cvTag = map.get("cv_tag");
                    String skillTag = map.get("skill_tag");
                    String personalTag = map.get("personal_tag");
                    String createdAt = map.get("created_at");
                    String updatedAt = map.get("updated_at");
                    RESUMES_EXTRAS_LOG.info("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}", id, compress, cvSource,
                        cvTrade, cvTitle, cvTag, skillTag, personalTag, createdAt, updatedAt);
                    idList.add(id);
                }
            }

            //algorithms
            for (String id : idList) {
                String algoSql = "select id,column_json(data) as data,updated_at,created_at from `" + db + "`.algorithms " +
                    "where id=" + id;
                LOG.info(algoSql);
                Map<String, String> algoMap = j % 2 == 0 ? mysql1.getAlgorithms(algoSql) : mysql2.getAlgorithms(algoSql);
                if (null != algoMap && algoMap.size() > 0) {
                    String algoId = algoMap.get("id");
                    String data = algoMap.get("data");
                    String createdAt = algoMap.get("created_at");
                    String updatedAt = algoMap.get("updated_at");
                    ALGORITHMS_LOG.info("{}\t{}\t{}\t{}", algoId, data, createdAt, updatedAt);
                }
            }

        }
        mysql1.close();
        mysql2.close();
        long t2 = System.currentTimeMillis();
        LOG.info("process icdc incre data use time:{}", (t2 - t1));
    }

}
