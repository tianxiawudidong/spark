package com.ifchange.spark.core.icdc;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.mysql.Mysql;
import com.ifchange.spark.util.ResumeUtil;
import com.ifchange.spark.util.Xxtea;
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

/**
 * 处理icdc中具有相同联系方式的简历id
 * 具体逻辑：
 * 1、如果id的compress 中contact为空，则将 resumes表的conatctId 置为0
 * 2、如果id的compress 中contact不为空
 * --2.1、比较compress contact的内容是否和contacts表一致，如果一致不做任何修改
 * --2.2、如果不相等，则重新生成一个新的contacts id，并修改resumes表的contactId
 * <p>
 * 不比较 直接生成一个新的contacts id，并修改resumes表的contactId
 */
public class ProcessSameContact {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessSameContact.class);

    private static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final String KEY = "!@#$%^&*()_+{\":}";

    private static final String HOST1 = "192.168.8.130";

    private static final String HOST2 = "192.168.8.132";

    private static final String USERNAME = "databus_user";

    private static final String PASSWORD = "Y,qBpk5vT@zVOwsz";

    private static final int PORT = 3306;

    public static void main(String[] args) throws Exception {
        String master = args[0];
        String appName = args[1];
        int partition = Integer.parseInt(args[2]);
        String path = args[3];

        if (args.length < 4) {
            throw new Exception("args length is not correct,check it");
        }

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        LongAccumulator contactProcessAcc = sparkSession.sparkContext().longAccumulator("contact_process_number");
        LongAccumulator contactEmptyAcc = sparkSession.sparkContext().longAccumulator("contact_empty_number");

        Dataset<String> result = sparkSession.read().textFile(path)
            .repartition(partition)
            .mapPartitions((MapPartitionsFunction<String, String>) iterator -> {
                List<String> list = new ArrayList<>();
                Mysql mysql130 = new Mysql(USERNAME, PASSWORD, "icdc_0", HOST1, PORT);
                Mysql mysql132 = new Mysql(USERNAME, PASSWORD, "icdc_1", HOST2, PORT);

                while (iterator.hasNext()) {
                    String line = iterator.next();
                    list.add(line);
                    if (StringUtils.isNoneBlank(line)) {
                        String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, ",");
                        if (split.length == 2) {
//                            long contactId = Long.parseLong(split[0]);
                            long resumeId = Long.parseLong(split[1]);
                            String dbName = ResumeUtil.getDBNameById2(resumeId);
                            int index = Integer.parseInt(dbName.split("_")[1]);
                            String sql = "select * from `" + dbName + "`.resumes_extras where id=" + resumeId;
                            LOG.info(sql);
                            String compress = index % 2 == 0 ? mysql130.queryCompress(sql)
                                : mysql132.queryCompress(sql);
                            if (StringUtils.isNoneBlank(compress)) {
                                try {
                                    JSONObject jsonObject = JSONObject.parseObject(compress);
                                    if (null != jsonObject) {
                                        JSONObject basic = jsonObject.getJSONObject("basic");
                                        JSONObject contact = jsonObject.getJSONObject("contact");
                                        if (null == contact || contact.size() == 0) {
                                            //直接更新resumes的contactId为0
                                            String updated = DTF.format(LocalDateTime.now());
                                            String updateResumeSql = "update `" + dbName + "`.resumes " +
                                                "set contact_id=0 , has_phone='N' , updated_at='" + updated + "' where id=" + resumeId;
                                            LOG.info(updateResumeSql);
                                            int flag = index % 2 == 0 ? mysql130.executeUpdate(updateResumeSql) :
                                                mysql132.executeUpdate(updateResumeSql);
                                            LOG.info("id:{} update resumes flag:{}", resumeId, flag);
                                            contactEmptyAcc.add(1L);
                                        } else {
                                            Map<String, Object> contactMap = new HashMap<>();
                                            String name = null != basic ? basic.getString("name") : "";
                                            String encodeTel = "";
                                            String tel = contact.getString("tel");
                                            if (StringUtils.isNoneBlank(tel)) {
                                                encodeTel = tel.length() < 12 ? Xxtea.encode(tel, KEY) : tel;
                                                LOG.info("tel:{} ->{}", tel, encodeTel);
                                            }
                                            String encodePhone = "";
                                            String phone = contact.getString("phone");
                                            if (StringUtils.isNoneBlank(phone)) {
                                                encodePhone = phone.length() < 12 ? Xxtea.encode(phone, KEY) : phone;
                                                LOG.info("phone:{} ->{}", phone, encodePhone);
                                            }
                                            String encodeEmail = "";
                                            String email = contact.getString("email");
                                            if (StringUtils.isNoneBlank(email)) {
                                                encodeEmail = email.contains("@") ? Xxtea.encode(email, KEY) : email;
                                                LOG.info("email:{} ->{}", email, encodeEmail);
                                            }
                                            String qq = null != contact.get("qq") ? contact.getString("qq") : "";
                                            String msn = null != contact.get("msn") ? contact.getString("msn") : "";
                                            String sina = null != contact.get("sina") ? contact.getString("sina") : "";
                                            String ten = null != contact.get("ten") ? contact.getString("ten") : "";
                                            String wechat = null != contact.get("wechat") ? contact.getString("wechat") : "";
                                            String phoneAreaStr = null != contact.get("phone_area")
                                                ? String.valueOf(contact.get("phone_area")) : "0";
                                            int phoneArea = 1;
                                            try {
                                                phoneArea = StringUtils.isNoneBlank(phoneAreaStr) ? Integer.parseInt(phoneAreaStr) : 1;
                                            } catch (NumberFormatException e) {
                                                LOG.error("phone_area:{} cannot parse to int", phoneAreaStr);
                                            }
                                            String isDeleted = null != contact.get("is_deleted") ? contact.getString("is_deleted") : "N";
                                            String time = DTF.format(LocalDateTime.now());
                                            contactMap.put("name", name);
                                            contactMap.put("tel", encodeTel);
                                            contactMap.put("phone", encodePhone);
                                            contactMap.put("email", encodeEmail);
                                            contactMap.put("qq", qq);
                                            contactMap.put("msn", msn);
                                            contactMap.put("sina", sina);
                                            contactMap.put("ten", ten);
                                            contactMap.put("wechat", wechat);
                                            contactMap.put("phone_area", phoneArea);
                                            contactMap.put("is_deleted", isDeleted);
                                            contactMap.put("updated_at", time);
                                            contactMap.put("created_at", time);

                                            //直接生成一个新的contact_id
                                            String getContactIdSql = "replace into `icdc_allot`.allots_contacts(`sub`) values('a')";
                                            int newContactId = index % 2 == 0 ? mysql130.executeInsert(getContactIdSql) :
                                                mysql132.executeInsert(getContactIdSql);
                                            contactMap.put("id", String.valueOf(newContactId));
                                            String contactDb = ResumeUtil.getDBNameById2(newContactId);
                                            //保存contact信息到contacts表
                                            String saveContactSql = "insert into `" + contactDb + "`.contacts(`id`,`name`,`tel`,`phone`,`email`," +
                                                "`qq`,`msn`,`sina`,`ten`,`wechat`,`phone_area`,`is_deleted`,`updated_at`,`created_at`)" +
                                                "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
                                            int index2 = Integer.parseInt(contactDb.split("_")[1]);
                                            int flag = index2 % 2 == 0 ? mysql130.saveContacts(saveContactSql, contactMap)
                                                : mysql132.saveContacts(saveContactSql, contactMap);
                                            LOG.info("id:{} save new contact_id:{},flag:{}", resumeId, newContactId, flag);

                                            //更新resumes主表
                                            String updated = DTF.format(LocalDateTime.now());
                                            String hasPhone = StringUtils.isNoneBlank(encodePhone) ? "Y" : "N";
                                            String updateResumeSql = "update `" + dbName + "`.resumes set contact_id="
                                                + newContactId + ", has_phone='" + hasPhone + "' , updated_at='" + updated
                                                + "' where id=" + resumeId;
                                            LOG.info(updateResumeSql);
                                            int flag2 = index % 2 == 0 ? mysql130.executeUpdate(updateResumeSql)
                                                : mysql132.executeUpdate(updateResumeSql);
                                            LOG.info("id:{} update resumes,flag:{}", resumeId, flag2);
                                            contactProcessAcc.add(1L);
                                        }
                                    }
                                } catch (Exception e) {
                                    LOG.error("id:{} compress cannot parse to json", resumeId);
                                }
                            }
                        } else {
                            LOG.error("{} length :{} is not correct", line, split.length);
                        }
                    }
                }
                mysql130.close();
                mysql132.close();
                return list.iterator();
            }, Encoders.STRING());

        long count = result.count();
        LOG.info("process number :{}", count);
        LOG.info("contact empty number:{}", contactEmptyAcc.value());
        LOG.info("contact process number:{}", contactProcessAcc.value());

    }

}
