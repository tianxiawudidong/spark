package com.ifchange.spark.core.icdc;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 过滤出icdc中具有相同联系方式的简历id
 */
public class FilterSameContactIds {

    private static final Logger LOG = LoggerFactory.getLogger(FilterSameContactIds.class);


    public static void main(String[] args) throws Exception {
        String master = args[0];
        String appName = args[1];
        int partition = Integer.parseInt(args[2]);
        String path = args[3];
        String savePath = args[4];
        int savePartition = Integer.parseInt(args[5]);

        if (args.length < 6) {
            throw new Exception("args length is not correct,check it");
        }

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        Dataset<Resumes> resumesDataset = sparkSession.read().textFile(path)
            .repartition(partition)
            .filter(new FilterFunction<String>() {
                @Override
                public boolean call(String s) throws Exception {
                    return StringUtils.isNoneBlank(s) &&
                        StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t").length > 2;
                }
            }).map(new MapFunction<String, Resumes>() {
                @Override
                public Resumes call(String s) throws Exception {
                    String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                    Resumes resumes = new Resumes();
                    long resumeId = 0;
                    long contactId = 0;
                    try {
                        resumeId = Long.parseLong(split[0]);
                        contactId = Long.parseLong(split[1]);
                    } catch (NumberFormatException e) {
                        LOG.error("{} parse error:{}", s, e.getMessage());
                    }
                    resumes.setResumeId(resumeId);
                    resumes.setContactId(contactId);
                    return resumes;
                }
            }, Encoders.bean(Resumes.class))
            .filter(new FilterFunction<Resumes>() {
                @Override
                public boolean call(Resumes resumes) throws Exception {
                    return resumes.getContactId() != 0L;
                }
            });


        resumesDataset.registerTempTable("resumes");

        Dataset<Row> result = sparkSession.sql("select distinct t1.contactId,r.resumeId " +
            " from (" +
            "  select contactId,count(r.resumeId) as number " +
            "  from resumes r group by r.contactId having count(r.resumeId)>1 )t1 " +
            "  join resumes r on t1.contactId=r.contactId where r.resumeId is not null order by t1.contactId ");

        result.repartition(savePartition)
            .write()
            .format("csv")
            .save(savePath);

        //只能保存1個列
//        result.repartition(savePartition).write().text(savePath);

    }

}
