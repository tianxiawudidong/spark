package com.ifchange.spark.algorithms.jc.ats;

import com.ifchange.spark.mysql.Mysql;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 处理recruit_step \t分隔后字段问题
 * /basic_data/tob/tob_ats/recruit_step_jc_init
 * 历史 7个字段 uid, icdc_position_id, resume_id, step_status, hr_status,cv_json,jd_json
 * 增量 8个字段 uid, icdc_position_id, resume_id, stage_type_id, stage_type_from, updated_at,cvJson,jdJson
 * ==》统一处理成8个字段
 * <p>
 * 2019/11/27
 */
public class ProcessRecruitStepSplit {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessRecruitStepSplit.class);

    private static final String HOST = "192.168.8.141";

    private static final int PORT = 3307;

    private static final String USER_NAME = "databus_user";

    private static final String PASS_WORD = "Y,qBpk5vT@zVOwsz";

    public static void main(String[] args) {
        if (args.length < 5) {
            LOG.info("args length is not correct");
            System.exit(-1);
        }

        String appName = args[0];
        String master = args[1];
        int partition = Integer.parseInt(args[2]);
        String path = args[3];
        String savePath = args[4];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        LongAccumulator accumulator = sparkSession.sparkContext().longAccumulator("recruit_step_number");
        LongAccumulator errAcc = sparkSession.sparkContext().longAccumulator("recruit_step_size_error");

        Dataset<String> results = sparkSession.read().textFile(path).repartition(partition)
            .mapPartitions((MapPartitionsFunction<String, String>) iterator -> {
                Mysql mysql = new Mysql(USER_NAME, PASS_WORD, "tob_ats_10", HOST, PORT);

                List<String> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    String str = iterator.next();
                    accumulator.add(1L);
                    if (StringUtils.isNoneBlank(str)) {
                        StringBuilder sb = new StringBuilder();
                        String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(str, "\t");
                        int length = split.length;
                        if (length == 7) {
                            //uid, icdc_position_id, resume_id, step_status, hr_status,cv_json,jd_json
                            String uid = split[0];
                            String icdcPositionId = split[1];
                            String resumeId = split[2];
                            String cvJson = split[5];
                            String jdJson = split[6];
                            //select * from `tob_ats_8`.recruit_step where uid=105805 and resume_id=81478312 limit 10\G;
                            Map<String, String> recruitStepMap = new HashMap<>();
                            for (int i = 1; i <= 10; i++) {
                                String db = "tob_ats_" + i;
                                String sql = "select * from `" + db + "`.recruit_step where uid=" + uid + " and resume_id="
                                    + resumeId + " limit 1";
                                LOG.info(sql);
                                recruitStepMap = mysql.getTobAtsRecruitStep(sql);
                                if (recruitStepMap.size() > 0) {
                                    break;
                                }
                            }
                            //替换step_status, hr_status -》stage_type_id, stage_type_from, updated_at
                            String stageTypeId = recruitStepMap.get("stage_type_id");
                            String stageTypeFrom = recruitStepMap.get("stage_type_from");
                            String updatedAt = recruitStepMap.get("updated_at");

                            //uid, icdc_position_id, resume_id, stage_type_id, stage_type_from, updated_at,cvJson,jdJson
                            sb.append(uid);
                            sb.append("\t");
                            sb.append(icdcPositionId);
                            sb.append("\t");
                            sb.append(resumeId);
                            sb.append("\t");
                            sb.append(stageTypeId);
                            sb.append("\t");
                            sb.append(stageTypeFrom);
                            sb.append("\t");
                            sb.append(updatedAt);
                            sb.append("\t");
                            sb.append(cvJson);
                            sb.append("\t");
                            sb.append(jdJson);
                        } else if (length == 8) {
                            //uid, icdc_position_id, resume_id, stage_type_id, stage_type_from, updated_at,cvJson,jdJson
                            sb.append(str);
                        } else {
                            errAcc.add(1L);
                            LOG.error("size:{} is not correct", length);
                        }
                        list.add(sb.toString());
                    }
                }
                mysql.close();
                return list.iterator();
            }, Encoders.STRING());

        //save text
        LOG.info("-------------------------------------------------------------------------------");
        LOG.info("process recruit_step_number:{}", accumulator.value());
        LOG.info("process recruit_step_size_error number:{}", errAcc.value());
        LOG.info("-------------------------------------------------------------------------------");
        results.write().text(savePath);

    }
}
