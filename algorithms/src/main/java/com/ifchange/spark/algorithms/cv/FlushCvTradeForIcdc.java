package com.ifchange.spark.algorithms.cv;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.algorithms.CvAlgorithmsTagUtil;
import com.ifchange.spark.algorithms.http.CropTagHttp;
import com.ifchange.spark.mysql.Mysql;
import com.ifchange.spark.util.MyString;
import com.ifchange.spark.util.ResumeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
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
import java.util.List;
import java.util.Map;


public class FlushCvTradeForIcdc {

    private static final Logger LOG = LoggerFactory.getLogger(FlushCvTradeForIcdc.class);

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
                CropTagHttp.init(5);
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
                                    String cvTrade = null;
                                    try {
                                        cvTrade = CvAlgorithmsTagUtil.callCvTrade(id, compress);
                                        LOG.info("id:{} cv_trade:{}", id, cvTrade);
                                    } catch (Exception e) {
                                        LOG.info("id:{} call cv_trade error:{}", id, e.getMessage());
                                    }
                                    if (StringUtils.isNoneBlank(cvTrade)) {
                                        cvTrade = MyString.toUnicode(cvTrade, false);
                                        cvTrade = cvTrade.replace("'", "\\'");
                                        String param = "'" + "cv_trade" + "'" + "," + "'" + cvTrade + "'";
                                        String updatedAt = dtf.format(LocalDateTime.now());
                                        try {
                                            String sql2 = "update `" + dbName + "`.algorithms set data=column_add(data," + param + ")," +
                                                "updated_at='" + updatedAt + "'  where id=" + id;
                                            LOG.info(sql2);
                                            int flag = index % 2 == 0 ? mysql130.executeUpdate(sql2)
                                                : mysql132.executeUpdate(sql2);
                                            LOG.info("id:{} update cv_trade flag:{}", id, flag);
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
                                        LOG.info("id:{} cv_trade result is empty", id);
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
        LOG.info("*************************************************************************");
        LOG.info("resume accumulator value:{}", resumeAccumulator.value());
        LOG.info("resume no cv_trade result accumulator value:{}", resumeWithNoWorkAccumulator.value());
        LOG.info("resume compress null and parse error value:{}", resumeCompressNullAccumulator.value());
        LOG.info("*************************************************************************");

    }

}
