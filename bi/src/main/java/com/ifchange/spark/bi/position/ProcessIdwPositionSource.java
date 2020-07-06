package com.ifchange.spark.bi.position;

import com.ifchange.spark.bi.bean.position.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * idw_positions.positions_source
 */
public class ProcessIdwPositionSource {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessIdwPositionSource.class);

    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String positionSourcePath = args[2];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse");

        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
        LongAccumulator totalAcc = sparkSession.sparkContext().longAccumulator("position_map_total_acc");
        LongAccumulator errorAcc = sparkSession.sparkContext().longAccumulator("position_map_error_acc");
        LongAccumulator rightAcc = sparkSession.sparkContext().longAccumulator("position_map_right_acc");

        Dataset<PositionsSources> positionSourceDs = sparkSession.read().textFile(positionSourcePath)
            .filter((FilterFunction<String>) s -> {
                totalAcc.add(1L);
                boolean flag = false;
                if (StringUtils.isNoneBlank(s)) {
                    String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                    if (split.length == 7) {
                        String isDeleted = split[4];
                        String positionIdStr = split[3];
                        if (StringUtils.equals("N", isDeleted) && StringUtils.isNoneBlank(positionIdStr)
                            && !StringUtils.equals("0", positionIdStr.trim())) {
                            flag = true;
                            rightAcc.add(1L);
                        } else {
                            errorAcc.add(1L);
                        }
                    } else {
                        errorAcc.add(1L);
                        LOG.error("position_sources size is wrong,{}", split.length);
                    }
                } else {
                    errorAcc.add(1L);
                    LOG.error("position_sources data is empty,{}", s);
                }
                return flag;
            }).map((MapFunction<String, PositionsSources>) s -> {
                PositionsSources positionsSources = new PositionsSources();
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                String id = split[0];
                try {
                    positionsSources.setId(Long.parseLong(id.trim()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                String sourceStr = split[1];
                long source = 0L;
                if (StringUtils.isNoneBlank(sourceStr)) {
                    try {
                        source = Long.parseLong(sourceStr.trim());
                    } catch (Exception e) {
                        LOG.error("source:{} parse long error:{}", sourceStr.trim(), e.getMessage());
                        e.printStackTrace();
                    }
                }
                positionsSources.setSource(source);

                String sourceId = split[2];
                positionsSources.setSource_id(sourceId);

                String positionIdStr = split[3];
                long positionId = 0L;
                if (StringUtils.isNoneBlank(positionIdStr)) {
                    try {
                        positionId = Long.parseLong(positionIdStr.trim());
                    } catch (Exception e) {
                        LOG.error("position_id:{} parse long error:{}", positionIdStr.trim(), e.getMessage());
                        e.printStackTrace();
                    }
                }
                positionsSources.setPosition_id(positionId);
                String isDeleted = split[4];
                positionsSources.setIsDeleted(isDeleted);
                String createdAt = split[5];
                positionsSources.setCreatedAt(createdAt);
                String updatedAt = split[6];
                positionsSources.setUpdatedAt(updatedAt);

                return positionsSources;
            }, Encoders.bean(PositionsSources.class));


        //joinType - Type of join to perform. Default inner. Must be one of: inner, cross, outer, full, full_outer, left, left_outer, right, right_outer, left_semi, left_anti
        Dataset<IdwPositionsSources> idwPositionsSourcesDs = positionSourceDs
            .select(positionSourceDs.col("source"),
                positionSourceDs.col("source_id"),
                positionSourceDs.col("position_id"),
                positionSourceDs.col("createdAt"))
            .map((MapFunction<Row, IdwPositionsSources>) row -> {
                IdwPositionsSources idwPositionsSources = new IdwPositionsSources();
                long source = row.getLong(0);
                idwPositionsSources.setSource(source);
                String sourceId = row.getString(1);
                idwPositionsSources.setSource_id(sourceId);
                long positionId = row.getLong(2);
                idwPositionsSources.setPosition_id(positionId);
                String createdAt = row.getString(3);
                idwPositionsSources.setCreated_at(createdAt);
                return idwPositionsSources;
            }, Encoders.bean(IdwPositionsSources.class));

        //save data into hive
        idwPositionsSourcesDs.write().mode(SaveMode.Overwrite).saveAsTable("idw_positions.positions_source");


    }
}
