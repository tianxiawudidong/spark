package com.ifchange.spark.core.jc;

import com.ifchange.spark.util.DateTimeUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 处理ats recruit_step(定制线、通用线) 每周合并增量
 */
public class ProcessRecruitStepWeeklyCompact {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessRecruitStepWeeklyCompact.class);

    public static void main(String[] args) throws Exception {

        String path = args[0];
        String savePath = args[1];
        String type = args[2];
        File file = new File(path);
        Map<String, List<String>> map = new HashMap<>();
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (null != files && files.length > 0) {
                for (File f1 : files) {
                    LOG.info("process file:{}", f1.getName());
                    BufferedReader br = new BufferedReader(new FileReader(f1));
                    String data;
                    while (StringUtils.isNoneBlank(data = br.readLine())) {
                        String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(data, "\t");
                        if (split.length == 9) {
                            //通用线
                            //uid、icdc_position_id、resume_id、tob_resume_id、stage_type_id、stage_type_from、updated_at、cv_json（tob_resume_id）、jd_json
                            //定制线
                            //uid、top_id、resume_id、tob_resume_id、icdc_position_id、step_type、updated_at、cv_json、jd_json
                            String uid = split[0];
                            String icdcPositionId = StringUtils.equals(type, "tyx") ? split[1] : split[4];
                            String tobResumeId = split[3];
                            String updatedAt = split[6];
                            String key = uid + "-" + icdcPositionId + "-" + tobResumeId;
                            if (!map.containsKey(key)) {
                                List<String> list = new ArrayList<>();
                                list.add(updatedAt);
                                list.add(data);
                                map.put(key, list);
                            } else {
                                //包含该key
                                List<String> list = map.get(key);
                                //比较map中的更新时间 和 data中的updated_at
                                String updatedAtMap = list.get(0);
                                //updatedAtMap 是否在updatedAt之前
                                boolean flag = DateTimeUtil.compareTime(updatedAtMap, updatedAt);
                                if (flag) {
                                    //更新
                                    List<String> newList = new ArrayList<>();
                                    newList.add(updatedAt);
                                    newList.add(data);
                                    map.put(key, newList);
                                }
                            }
                        }
                    }
                    br.close();
                }
            }
        }

        File saveFile = new File(savePath);
        if (!saveFile.exists()) {
            saveFile.createNewFile();
        }
        BufferedWriter bw = new BufferedWriter(new FileWriter(saveFile));
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            List<String> list = entry.getValue();
            String value = list.get(1);
            bw.write(value);
            bw.newLine();
        }
        bw.flush();
        bw.close();
    }

}
