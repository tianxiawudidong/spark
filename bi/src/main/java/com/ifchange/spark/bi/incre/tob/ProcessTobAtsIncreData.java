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
 * 处理tob ats_* delivery 增量数据
 * <p>
 */
public class ProcessTobAtsIncreData {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessTobAtsIncreData.class);

    private static final Logger DELIVERY_INCRE_LOG = LoggerFactory.getLogger("DELIVERY_INCRE_LOGGER");

    private static final String HOST = "192.168.9.252";

    private static final String USERNAME = "hadoop_user";

    private static final String PASSWORD = "98ktzuhTtqmWWh28";

    private static final int PORT = 3306;

    private static final String DB_NAME = "tob_ats_1";

    private static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");


    public static void main(String[] args) throws Exception {
        LOG.info("init mysql");
        Mysql mysql = new Mysql(USERNAME, PASSWORD, DB_NAME, HOST, PORT);

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

        for (int j = 1; j < 11; j++) {
            String db = "tob_ats_" + j;
            String sql = "select delivery_id,tob_position_id,icdc_position_id,tob_resume_id,created_at,updated_at,is_deleted" +
                " from `" + db + "`.delivery where updated_at>='" + startTime + "' and updated_at<='" + endTime + "'";
            LOG.info(sql);
            List<Map<String, String>> list = mysql.getTobAtsDelivery(sql);
            if (null != list && list.size() > 0) {
                for (Map<String, String> map : list) {
                    String deliveryId = map.get("delivery_id");
                    String tobPositionId = map.get("tob_position_id");
                    String icdcPositionId = map.get("icdc_position_id");
                    String tobResumeId = map.get("tob_resume_id");
                    String updatedAt = map.get("updated_at");
                    String createdAt = map.get("created_at");
                    String isDeleted = map.get("is_deleted");
                    DELIVERY_INCRE_LOG.info("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}", deliveryId, tobPositionId, icdcPositionId,
                        tobResumeId, createdAt, updatedAt, isDeleted, j);
                }
            }
        }
        mysql.close();

    }


}
