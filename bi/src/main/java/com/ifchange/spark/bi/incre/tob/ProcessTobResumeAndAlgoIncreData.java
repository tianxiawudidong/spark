package com.ifchange.spark.bi.incre.tob;

import com.ifchange.spark.mysql.Mysql;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 处理tob 简历 算法 增量数据
 *
 * <p>
 */
public class ProcessTobResumeAndAlgoIncreData {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessTobResumeAndAlgoIncreData.class);

    private static final Logger RESUME_DETAIL_INCRE_LOG = LoggerFactory.getLogger("RESUME_DETAIL_INCRE_LOGGER");

    private static final Logger ALGO_INCRE_LOG = LoggerFactory.getLogger("ALGO_INCRE_LOGGER");

    private static final String HOST_1 = "192.168.9.135";

    private static final String HOST_2 = "192.168.9.168";

    private static final String USERNAME = "dataapp_user";

    private static final String PASSWORD_1 = "Q6pXGo8cP3";

    private static final String PASSWORD_2 = "lz4PWZDK/aMWz";

    private static final int PORT = 3306;

    private static final String DB_NAME_1 = "tob_algorithms_pool_0";

    private static final String DB_NAME_2 = "tob_algorithms_pool_4";

    private static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");


    public static void main(String[] args) throws Exception {
        LOG.info("init mysql");
        Mysql mysql135 = new Mysql(USERNAME, PASSWORD_1, DB_NAME_1, HOST_1, PORT);
        Mysql mysql168 = new Mysql(USERNAME, PASSWORD_2, DB_NAME_2, HOST_2, PORT);

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


        for (int i = 0; i < 8; i++) {
            for (int j = 0; j < 8; j++) {
                String resumeDb = "tob_resume_pool_" + i;
                String resumeTable = "resume_detail_" + j;
                String algoDb = "tob_algorithms_pool_" + i;
                String algoTable = "algorithms_" + j;
                String resumeSql = "select tob_resume_id,resume_content,created_at,updated_at from `" + resumeDb
                    + "`." + resumeTable + " where updated_at>='" + startTime + "' and updated_at<='" + endTime + "'";
                LOG.info(resumeSql);
                List<Map<String, String>> resumeList = i < 4 ? mysql135.getTobResumeDetail(resumeSql) :
                    mysql168.getTobResumeDetail(resumeSql);
                if (null != resumeList && resumeList.size() > 0) {
                    for (Map<String, String> resumeMap : resumeList) {
                        String tobResumeId = resumeMap.get("tob_resume_id");
                        String resumeContent = resumeMap.get("resume_content");
                        String updatedAt = resumeMap.get("updated_at");
                        String createdAt = resumeMap.get("created_at");
                        RESUME_DETAIL_INCRE_LOG.info("{}\t{}\t{}\t{}", tobResumeId, resumeContent, createdAt, updatedAt);
                    }
                }
                String algoSql = "select tob_resume_id,column_json(data) as data,updated_at,created_at from `" + algoDb
                    + "`." + algoTable + " where updated_at>='" + startTime + "' and updated_at<='" + endTime + "'";
                LOG.info(algoSql);
                List<Map<String, String>> algorithmsList = i < 4 ? mysql135.getTobAlgorithms(algoSql) :
                    mysql168.getTobAlgorithms(algoSql);
                if (null != algorithmsList && algorithmsList.size() > 0) {
                    for (Map<String, String> algorithmsMap : algorithmsList) {
                        String tobResumeId = algorithmsMap.get("tob_resume_id");
                        String data = algorithmsMap.get("data");
                        String updatedAt = algorithmsMap.get("updated_at");
                        String createdAt = algorithmsMap.get("created_at");
                        ALGO_INCRE_LOG.info("{}\t{}\t{}\t{}", tobResumeId, data, createdAt, updatedAt);
                    }
                }
            }
        }
        mysql135.close();
        mysql168.close();


    }


}
