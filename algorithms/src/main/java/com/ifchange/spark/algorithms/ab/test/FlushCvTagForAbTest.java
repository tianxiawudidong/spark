package com.ifchange.spark.algorithms.ab.test;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.algorithms.CvAlgorithmsTagUtil;
import com.ifchange.spark.algorithms.http.CvTagHttpForAbTest;
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
import java.util.List;
import java.util.Map;

/**
 * AB test
 * B环境刷cv_tag
 * 使用column_add 替换 column_create
 * column_create(param) 会覆盖字段里原先的值
 * column_add新增或更新
 */
public class FlushCvTagForAbTest {

    private static final Logger LOG = LoggerFactory.getLogger(FlushCvTagForAbTest.class);

    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final String USERNAME = "databus_user";

    private static final String PASSWORD = "123456";

    private static final String HOST = "10.9.10.57";

    private static final int PORT = 3310;

    public static void main(String[] args) {

        if (args.length < 5) {
            LOG.info("args length is not correct");
            System.exit(-1);
        }
        String appName = args[0];
        String master = args[1];
        int partition = Integer.parseInt(args[2]);
        String path = args[3];
        final String url = args[4];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        LongAccumulator resumeAccumulator = sparkSession.sparkContext().longAccumulator("resume_accumulator");
        LongAccumulator resumeNoWorkAccumulator = sparkSession.sparkContext().longAccumulator("resume_no_work_accumulator");
        LongAccumulator resumeCompressErrorAccumulator = sparkSession.sparkContext().longAccumulator("resume_compress_error_accumulator");

        Dataset<String> result = sparkSession.read().textFile(path).repartition(partition)
            .mapPartitions((MapPartitionsFunction<String, String>) iterator -> {
                CvTagHttpForAbTest.init(2, url);
                Mysql mysql = new Mysql(USERNAME, PASSWORD, "icdc_0", HOST, PORT);
                List<String> list = new ArrayList<>();

                while (iterator.hasNext()) {
                    String resumeId = iterator.next();
                    list.add(resumeId);
                    if (StringUtils.isNoneBlank(resumeId)) {
                        long id = Long.parseLong(resumeId.trim());
                        String db = ResumeUtil.getDBNameById(id);
                        String sql = "select compress from `" + db + "`.resumes_extras where id=" + id;
                        LOG.info(sql);
                        String compressStr = null;
                        try {
                            compressStr = mysql.queryCompress(sql);
                        } catch (Exception e) {
                            LOG.info("id:{} query compress error:{} ", id, e.getMessage());
                        }
                        if (StringUtils.isNoneBlank(compressStr)) {
                            Map<String, Object> compress = null;
                            try {
                                compress = JSONObject.parseObject(compressStr);
                            } catch (Exception e) {
                                LOG.info("id:{} compress parse json error:{}", id, e.getMessage());
                            }
                            if (null != compress) {
                                try {
                                    String cvTagResult = null;
                                    try {
                                        cvTagResult = CvAlgorithmsTagUtil.callCvTagForAbTest(resumeId, compress);
                                    } catch (Exception e) {
                                        LOG.info("id:{} call cv_tag error:{}", id, e.getMessage());
                                    }
                                    if (StringUtils.isNoneBlank(cvTagResult)) {
                                        String s = MyString.toUnicode(cvTagResult, false);
                                        s = s.replace("'", "\\'");
                                        String param = "'cv_tag','" + s + "'";
                                        String time = dtf.format(LocalDateTime.now());
                                        //保存到algorithms
//                                            String algorithmsSql = " replace into `" + db + "`.algorithms (`id`, updated_at, created_at, data) values(" +
//                                                    id + ",'" + time + "','" + time + "',column_create(" + param + "))";
                                        String algorithmsSql = "update `" + db + "`.algorithms set data=column_add(data," + param + "),updated_at='" + time + "' where id=" + id;
                                        mysql.execute(algorithmsSql);
                                    }
                                } catch (Exception e) {
                                    LOG.info("id:{} save to algorithms error:{}", id, e.getMessage());
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                }
                mysql.close();
                return list.iterator();
            }, Encoders.STRING());

        long count = result.count();
        LOG.info("process number:{}", count);
        LOG.info("*************************************************************************");
        LOG.info("resume accumulator value:{}", resumeAccumulator.value());
        LOG.info("resume no work accumulator value:{}", resumeNoWorkAccumulator.value());
        LOG.info("resume compress parse error value:{}", resumeCompressErrorAccumulator.value());
        LOG.info("*************************************************************************");


    }


}
