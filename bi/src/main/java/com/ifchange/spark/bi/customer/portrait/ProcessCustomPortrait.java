package com.ifchange.spark.bi.customer.portrait;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ProcessCustomPortrait {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessCustomPortrait.class);

    public static void main(String[] args) {

        String master = args[0];
        String appName = args[1];
        String positionPath = args[2];
        int partition = Integer.parseInt(args[3]);
        String corpPath = args[4];
        int corpPartition = Integer.parseInt(args[5]);
        String savePath = args[6];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse");
        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        Map<Integer, String> siteMap = new HashMap<>();
        Map<String, Integer> cityMap = new HashMap<>();
        siteMap.put(1, "智联");
        siteMap.put(2, "51job");
        siteMap.put(3, "猎聘");
        siteMap.put(4, "大街");
        siteMap.put(7, "中国人才热线");
        siteMap.put(9, "中华英才网");
        siteMap.put(11, "拉勾");
        siteMap.put(12, "58同城");
        siteMap.put(13, "赶集");
        siteMap.put(20, "领英");
        siteMap.put(21, "中国金融人才网");
        siteMap.put(22, "新安人才网");
        siteMap.put(23, "成都人才网");
        siteMap.put(24, "聘宝");
        siteMap.put(25, "找萝");
        siteMap.put(26, "人才啊");
        siteMap.put(27, "看准网");
        siteMap.put(28, "桔子网");
        siteMap.put(30, "猎上网");
        siteMap.put(31, "卓聘");
        siteMap.put(32, "昆山人才网");
        siteMap.put(33, "Boss直聘");
        siteMap.put(34, "建筑英才网");
        siteMap.put(35, "简历咖");
        siteMap.put(36, "妙招网");
        siteMap.put(37, "纷简历");
        siteMap.put(38, "脉脉");
        siteMap.put(39, "维基百科");
        siteMap.put(40, "新浪微博");
        siteMap.put(41, "职友圈");
        siteMap.put(42, "天眼查");
        siteMap.put(43, "猎萝");
        siteMap.put(44, "卓博");
        siteMap.put(45, "实习僧");
        siteMap.put(46, "汇博");
        siteMap.put(47, "温州人力资源网");
        siteMap.put(48, "服装人才网");
        siteMap.put(49, "牧通人才网");
        siteMap.put(51, "汽车人才网");
        siteMap.put(52, "中国医疗人才网");
        siteMap.put(53, "北极星");
        siteMap.put(54, "无忧精英");
        siteMap.put(55, "斗米");
        siteMap.put(56, "百姓网");
        siteMap.put(57, "店长直聘");
        siteMap.put(58, "优蓝网");
        siteMap.put(59, "丁香园 (备用)");
        siteMap.put(60, "厦门人才网");
        siteMap.put(10001, "全国统一社会信用代码信息核查系统");
        siteMap.put(10002, "知乎");
        siteMap.put(10003, "丁香园");
        //===========
        cityMap.put("上海", 1);
        cityMap.put("深圳", 2);
        cityMap.put("北京", 3);
        cityMap.put("武汉", 4);
        cityMap.put("重庆", 5);
        cityMap.put("苏州", 6);
        cityMap.put("杭州", 7);
        cityMap.put("南京", 8);
        cityMap.put("成都", 9);
        cityMap.put("天津", 10);
        cityMap.put("石家庄", 11);
        cityMap.put("太原", 12);
        cityMap.put("西安", 13);
        cityMap.put("济南", 14);
        cityMap.put("郑州", 15);
        cityMap.put("沈阳", 16);
        cityMap.put("长春", 17);
        cityMap.put("哈尔滨", 18);
        cityMap.put("合肥", 19);
        cityMap.put("南昌", 20);
        cityMap.put("福州", 21);
        cityMap.put("长沙", 22);
        cityMap.put("贵阳", 23);
        cityMap.put("昆明", 24);
        cityMap.put("广州", 25);
        cityMap.put("海口", 26);
        cityMap.put("兰州", 27);
        cityMap.put("西宁", 28);
        cityMap.put("呼和浩特", 29);
        cityMap.put("乌鲁木齐", 30);
        cityMap.put("拉萨", 31);
        cityMap.put("南宁", 32);
        cityMap.put("银川", 33);

        SparkContext sparkContext = sparkSession.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sparkContext);
        Broadcast<Map<String, Integer>> cityBroad = jsc.broadcast(cityMap);
        Broadcast<Map<Integer, String>> siteBroad = jsc.broadcast(siteMap);


        Dataset<Position> positionDataset = sparkSession.read()
            .textFile(positionPath)
            .repartition(partition)
            .filter(new FilterFunction<String>() {
                @Override
                public boolean call(String s) throws Exception {
                    boolean flag = false;
                    if (StringUtils.isNotBlank(s)) {
                        String[] stringArray = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                        if (stringArray.length == 31) {
                            flag = true;
                        }
                    }
                    return flag;
                }
            })
            .map(new MapFunction<String, Position>() {
                @Override
                public Position call(String s) throws Exception {
                    Position position = new Position();
                    String[] stringArray = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");

                    position.setId(Integer.valueOf(stringArray[0]));
                    position.setSiteId(Integer.valueOf(stringArray[1]));
                    position.setSiteUnique(stringArray[2]);
                    position.setSourceUnique(stringArray[3]);
                    position.setName(stringArray[4]);
                    position.setCorporationId(Integer.valueOf(stringArray[5]));
                    position.setCorporationName(stringArray[6]);
                    position.setSalary(stringArray[7]);
                    position.setCity(stringArray[8]);
                    position.setPublishTime(stringArray[9]);
                    position.setParsePublishTime(stringArray[10]);
                    position.setBody(stringArray[11]);
                    position.setStatus(Integer.valueOf(stringArray[12]));
                    position.setStatusToh(Integer.valueOf(stringArray[13]));
                    position.setPositionId(Integer.valueOf(stringArray[14]));
                    position.setPositionTohId(Integer.valueOf(stringArray[15]));
                    position.setImportedAt(stringArray[16]);
                    position.setIsDeleted(Integer.valueOf(stringArray[17]));
                    position.setUpdatedAt(stringArray[18]);
                    position.setCreatedAt(stringArray[19]);
                    position.setLanguage(stringArray[20]);
                    position.setEducation(stringArray[21]);
                    position.setRecruitNum(Integer.valueOf(stringArray[22]));
                    position.setWelfare(stringArray[23]);
                    position.setDescription(stringArray[24]);
                    position.setSalaryBegin(Integer.valueOf(stringArray[25]));
                    position.setSalaryEnd(Integer.valueOf(stringArray[26]));
                    position.setRecruitNumUp(Integer.valueOf(stringArray[27]));
                    position.setCityIds(stringArray[28]);
                    position.setFunctionIds(stringArray[29]);
                    position.setFunctionNames(stringArray[30]);
                    return position;
                }
            }, Encoders.bean(Position.class));

        positionDataset.registerTempTable("positions");

        Dataset<Corporation> corpDataSet = sparkSession.read()
            .textFile(corpPath)
            .repartition(corpPartition)
            .filter(new FilterFunction<String>() {
                @Override
                public boolean call(String s) throws Exception {
                    boolean flag = false;
                    if (StringUtils.isNotBlank(s)) {
                        String[] stringArray = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                        if (stringArray.length == 20) {
                            flag = true;
                        }
                    }
                    return flag;
                }
            })
            .map(new MapFunction<String, Corporation>() {
                @Override
                public Corporation call(String s) throws Exception {
                    Corporation corporation = new Corporation();
                    String[] stringArray = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "\t");
                    corporation.setId(Integer.valueOf(stringArray[0]));
                    corporation.setCorporationId(Integer.valueOf(stringArray[1]));
                    corporation.setSiteId(Integer.valueOf(stringArray[2]));
                    corporation.setSiteUnique(stringArray[3]);
                    corporation.setName(stringArray[4]);
                    corporation.setRealName(stringArray[5]);
                    corporation.setIndustry(stringArray[6]);
                    corporation.setIndustryIds(stringArray[7]);
                    corporation.setFinancing(stringArray[8]);
                    corporation.setScale(stringArray[9]);
                    corporation.setType(stringArray[10]);
                    corporation.setHomepage(stringArray[11]);
                    corporation.setAddress(stringArray[12]);
                    corporation.setStatus(Integer.valueOf(stringArray[13]));
                    corporation.setIsDeleted(Integer.valueOf(stringArray[14]));
                    corporation.setUpdatedAt(stringArray[15]);
                    corporation.setCreatedAt(stringArray[16]);
                    corporation.setBody(stringArray[17]);
                    corporation.setLogo(stringArray[18]);
                    corporation.setContact(stringArray[19]);
                    return corporation;
                }
            }, Encoders.bean(Corporation.class));

        corpDataSet.registerTempTable("corporations");


        Dataset<Row> joinSqlResultDF = sparkSession.sql("select max(t_id),positionId,siteId,city,positionName,recruitNum,link,corporationId,corporationName,industryIds,scale, description, functionIds, functionNames \n" +
            "from (\n" +
            "    select (case when isnull(t2.id)=false then t2.id else 0 end) as t_id, t1.*, t2.industryIds, t2.scale \n" +
            "    from (\n" +
            "        select id as positionId,siteId,city,name positionName,recruitNum,body as link, (case when corporationId > 0 then corporationId else -1 end) as corporationId, corporationName, description, functionIds, functionNames \n" +
            "        from positions \n" +
            "        ) t1 left join corporations t2 on t1.corporationId = t2.corporationId and t1.siteId = t2.siteId ) tt\n" +
            "group by positionId,siteId,city,positionName,recruitNum,link,corporationId,corporationName,industryIds,scale,description,functionIds, functionNames ");


        Dataset<String> result = joinSqlResultDF.mapPartitions(new MapPartitionsFunction<Row, String>() {
            @Override
            public Iterator<String> call(Iterator<Row> iterator) throws Exception {
                LOG.info("___________begin_______________");

                CorpHttp corpHttp = new CorpHttp();
                GsystemHttp gsystemHttp = new GsystemHttp();
                FunctionHttp functionHttp = new FunctionHttp();

                // 指定城市、公司、行业的数据
                // ArrayList<String> insertValues = new ArrayList<>();
                // 指定城市、公司的数据
                ArrayList<String> insertValuesAll = new ArrayList<>();

                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    Object positionId = row.get(1);

                    Object siteId = row.get(2);
                    if (siteId == null || StringUtils.isBlank(siteId.toString())) {
                        siteId = -1;
                    }
                    String siteName = siteBroad.getValue().getOrDefault(siteId, "-");

                    Object city = row.get(3);
                    if (city == null || StringUtils.isBlank(city.toString())) {
                        city = "";
                    }
                    ArrayList<String> itemCities = new ArrayList<>();
                    Iterator<String> cityIterator = cityBroad.getValue().keySet().iterator();
                    while (cityIterator.hasNext()) {
                        String key = cityIterator.next();
                        if (city.toString().contains(key)) {
                            itemCities.add(key);
                        }
                    }
                    if (itemCities.isEmpty()) {
                        continue;
                    }

                    Object positionName = row.get(4);
                    if (positionName == null || StringUtils.isBlank(positionName.toString())) {
                        positionName = "";
                    }

                    Object recruitNum = row.get(5);

                    Object link = row.get(6);

                    Object corporationName = row.get(8);

                    if (corporationName == null || StringUtils.isBlank(corporationName.toString())) {
                        continue;
                    }

                    Object[] corpInfo = corpHttp.getCorp(corporationName.toString());
                    Integer corporationId = (Integer) corpInfo[0];
                    String corpName = corpInfo[1].toString();

                    // boolean flag = CompanyIds.containCompanyId(corporationId.toString());

                    Object industryIds = row.get(9);
                    ArrayList<Integer> industryList = new ArrayList<>();
                    if (industryIds == null || StringUtils.isBlank(industryIds.toString())) {
                        industryList.add(-1);
                    } else {
                        String[] list = industryIds.toString().replaceAll("，", ",").replaceAll(" ", "").split(",");
                        for (String i : list) {
                            if (StringUtils.isNotBlank(i)) {
                                industryList.add(Integer.valueOf(i));
                            }
                        }
                    }

                    Object scale = row.get(10);
                    if (scale == null || StringUtils.isBlank(scale.toString())) {
                        scale = "-";
                    }

                    Object description = row.get(11);
                    Object functionIds = row.get(12);
                    Object functionNames = row.get(13);
                    Integer functionId = null;
                    String functionName = null;
                    Object[] function = null;

                    if (functionIds != null) {
                        if (StringUtils.isNotBlank(functionIds.toString()) && StringUtils.isNotEmpty(functionIds.toString())) {
                            String[] strArr = StringUtils.splitByWholeSeparatorPreserveAllTokens(functionIds.toString(), ",");
                            if (strArr.length == 3) {
                                functionId = Integer.valueOf(strArr[1]);
                            }
                        } else {
                            function = functionHttp.getFunction(positionName.toString(), description.toString());
                            functionId = function[0] == null ? 0 : (Integer) function[0];

                        }
                    }

                    if (functionNames != null) {
                        if (StringUtils.isNotBlank(functionNames.toString()) && StringUtils.isNotEmpty(functionNames.toString())) {
                            String[] strArr = StringUtils.splitByWholeSeparatorPreserveAllTokens(functionNames.toString(), ",");
                            if (strArr.length == 3) {
                                functionName = strArr[1];
                            }
                        } else {
                            functionName = function[1] == null ? "-" : function[1].toString();
                        }
                    }

                    for (String cName : itemCities) {
                        for (Integer industryId : industryList) {
                            String industryName = gsystemHttp.getIndustry(industryId);
                            StringBuilder sbAll = new StringBuilder();

                            sbAll.append(positionId).append("\t");
                            sbAll.append(siteId).append("\t");
                            sbAll.append(siteName).append("\t");
                            sbAll.append(industryId).append("\t");
                            sbAll.append(industryName).append("\t");
                            sbAll.append(corporationId).append("\t");
                            sbAll.append(corpName).append("\t");
                            sbAll.append(scale).append("\t");
                            sbAll.append(cityBroad.getValue().getOrDefault(cName, -1)).append("\t");
                            sbAll.append(cName).append("\t");
                            sbAll.append(positionName).append("\t");
                            sbAll.append(functionId).append("\t");
                            sbAll.append(recruitNum).append("\t");
                            sbAll.append(link).append("\t");
                            sbAll.append(functionName);
                            insertValuesAll.add(sbAll.toString());
                        }
                    }
                }
                LOG.info("___________end_________________");
                return insertValuesAll.iterator();
            }
        }, Encoders.STRING());

        result.repartition(50).write().text(savePath);


    }
}
