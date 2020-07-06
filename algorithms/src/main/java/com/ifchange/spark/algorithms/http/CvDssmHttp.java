package com.ifchange.spark.algorithms.http;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.util.ExecutorFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

public class CvDssmHttp extends AlgorithmsHttp implements Callable<String> {

//    private static final String url = "http://10.9.10.23:51900/dssm";

    private static final String url = "http://algo.rpc/dssm";

    private Map<String, Object> pMap = new LinkedHashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(CvDssmHttp.class);

    private static ThreadPoolExecutor threadPoolExecutor;

    private static final int ThreadNumber = 2;


    public CvDssmHttp(String resumeId, Map<String, Object> compress) throws Exception {
        super();
        if (null == threadPoolExecutor) {
            throw new Exception("thread pool not init,please call init method");
        }
        requestMap.put("c", "");
        requestMap.put("m", "cv");
        requestMap.put("p", pMap);
        super.headerMap.put("log_id", String.format("%s_%d_%d", resumeId, System.currentTimeMillis(), Thread.currentThread().getId()));
        try {
            processCompress(resumeId, compress);
        } catch (Exception e) {
            logger.error("{},msg,{}", resumeId, e.getMessage());
        }
    }

    private void processCompress(String resumeId, Map<String, Object> compress) throws Exception {
        Object workObj = compress.get("work");
        Map<String, String> titleMap = new HashMap<>();
        Map<String, String> descMap = new HashMap<>();
        Map<String, Object> work = new HashMap<>();
        if (workObj instanceof Map) {
            work = (Map<String, Object>) workObj;
        }
        if (work.isEmpty()) {
            throw new Exception(String.format("resume_id:%s work is null or empty", resumeId));
        }
        for (Map.Entry<String, Object> entry : work.entrySet()) {
            String workId = entry.getKey();
            Map<String, Object> workDetail = (Map<String, Object>) entry.getValue();
            String title = null != workDetail.get("position_name") ? String.valueOf(workDetail.get("position_name")) : "";
            if (StringUtils.isNoneBlank(title)) {
                titleMap.put(workId, title);
            }
            String responsibilities = null != workDetail.get("responsibilities") ? String.valueOf(workDetail.get("responsibilities")) : "";
            if (StringUtils.isNoneBlank(responsibilities)) {
                descMap.put(workId, responsibilities);
            }
        }
        if (titleMap.size() > 0) {
            pMap.put("ti", titleMap);
        }

        if (descMap.size() > 0) {
            pMap.put("desc", descMap);
        }
    }


    @Override
    public String call() {
        String result = "";
        if (null != pMap && pMap.size() > 0) {
            String param = JSON.toJSONString(msgMap);
            logger.info("param:{}", param);
            HttpClientBuilder hb = HttpClientBuilder.create();
            CloseableHttpClient client = hb.build();
            HttpPost method = new HttpPost(url);
            RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(5000)//建立连接的超时时间
                .setConnectionRequestTimeout(1000)
                .setSocketTimeout(3000)//指客户端和服务进行数据交互的时间，是指两者之间如果两个数据包之间的时间大于该时间则认为超时
                .build();
            method.setConfig(requestConfig);

            //解决中文乱码问题
            StringEntity entity = new StringEntity(param, "utf-8");
            entity.setContentEncoding("UTF-8");
            entity.setContentType("application/json");
            method.setEntity(entity);
            try {
                HttpResponse httpResponse = client.execute(method);
                //请求发送成功，并得到响应
                if (httpResponse.getStatusLine().getStatusCode() == 200) {
                    // 读取服务器返回过来的json字符串数据
                    result = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
                }
                client.close();
            } catch (IOException e) {
                logger.info("cv_dssm call http error:{},param:{}", e.getMessage(), param);
            }
        }
        return result;
    }

    public String start(CvDssmHttp cvDssmHttp) throws Exception {
        Future<String> submit = threadPoolExecutor.submit(cvDssmHttp);
        return submit.get();
    }

    public static void init() {
        threadPoolExecutor = (ThreadPoolExecutor) ExecutorFactory.newFixedThreadPool(ThreadNumber);
    }

    public static void init(int threadNumber) {
        threadPoolExecutor = (ThreadPoolExecutor) ExecutorFactory.newFixedThreadPool(threadNumber);
    }

    public static void main(String[] args) {
        CvDssmHttp.init();
        String compress = "{\"education\":{\"C57368AA175C5B19BC71F591C4700630\":{\"discipline_name\":\"视觉传达设计\",\"start_time\":\"2012年9月\",\"so_far\":\"N\",\"degree_origin_txt\":\"郑州商学院 本科 | 视觉传达设计\",\"discipline_desc\":\"\",\"end_time\":\"2016年6月\",\"degree\":\"1\",\"school_name\":\"郑州商学院\",\"id\":\"C57368AA175C5B19BC71F591C4700630\",\"degree_origin\":\"本科\",\"sort_id\":0}},\"work\":{\"0EEC0538CC4AC6228AA8D4ECFF3CE815\":{\"start_time\":\"2016年8月\",\"so_far\":\"N\",\"industry_name\":\"计算机软件\",\"responsibilities\":\"工作描述：\\n1、负责iOS、Android、PC等设备操作系统的视觉设计以及交互设计\\n2、主导设定整体视觉风格设计，包括动效、插图等\\n3、负责参与设计体验、流程等制定和规范\\n4、分析设计经验、提高团队的设计能力\\n5、完成相关运营设计，动效demo，产品包装等\\n6、与整个部门紧密合作，协同达成最终产品目标。\\n7、完成部门安排的其他工作\",\"end_time\":\"2019年8月\",\"position_name\":\"UI设计师\",\"scale\":\"150-500人\",\"corporation_name\":\"上海盛大网络发展有限公司\",\"id\":\"0EEC0538CC4AC6228AA8D4ECFF3CE815\",\"corporation_type\":\"民营公司\",\"sort_id\":0}},\"contact\":{\"phone\":\"17521721563\",\"email_origin\":\"rimutuyuanleo@foxmail.com\",\"name\":\"徐兵\",\"phone_origin\":\"17521721563\",\"email\":\"rimutuyuanleo@foxmail.com\"},\"skill\":[{\"level\":\"Photoshop熟练\",\"name\":\"Photoshop\"},{\"level\":\"Illustrator熟练\",\"name\":\"Illustrator\"},{\"level\":\"AUTOCAD良好\",\"name\":\"AUTOCAD\"}],\"certificate\":{\"DD90E0DD221A21F8D11EE5478CD7AFE0\":{\"start_time\":\"2014年6月\",\"name\":\"大学英语四级\",\"id\":\"DD90E0DD221A21F8D11EE5478CD7AFE0\",\"sort_id\":0}},\"project\":{\"2188DCF834F904FDCDD85A8D1E5DCF3A\":{\"start_time\":\"2019年3月\",\"so_far\":\"N\",\"responsibilities\":\"1、分析产品功能与交互，并对应提出相应专业意见。\\n2、收集相应素材，并输出产品原型，交互说明、设计稿与设计规范\\n3、配合开发相应工作，规范设计质量。\\n4、负责对应对运营设计等\",\"end_time\":\"2019年7月\",\"name\":\"登辰手游app\",\"describe\":\"手游分享社区应用，为玩家提供手游下载，互动社区，攻略，开测信息，福利礼包等多样游戏服务。\",\"id\":\"2188DCF834F904FDCDD85A8D1E5DCF3A\",\"sort_id\":0},\"3F923727693FE18766D1C1351085FF0C\":{\"start_time\":\"2018年8月\",\"so_far\":\"N\",\"responsibilities\":\"1、 对用户体验、用户需求进行梳理，并对竞品数据及行业数据进行收集分析\\n2、参与产品需求的梳理，并输出产品原型、流程图、交互文档等\\n3、 负责制定UI及交互设计规范，跟进设计质量\\n4、 参与产品的包装设计和动态DEMO的实现等\\n（项目查看链接https://www.zcool.com.cn/work/ZMzYyMTEzODA=/1.html）\",\"end_time\":\"2019年2月\",\"name\":\"失恋屋APP\",\"describe\":\"公司孵化的社交类项目，以情感受挫年轻人为垂直用户，以期解决社交困难与社交恐惧，打造全新概念，全新风格的亲密社交社区。\",\"id\":\"3F923727693FE18766D1C1351085FF0C\",\"sort_id\":1},\"BD6D009342A87BBB5CE1624918C87165\":{\"start_time\":\"2018年2月\",\"so_far\":\"N\",\"responsibilities\":\"1、对项目相关功能、页面架构与交互逻辑进行探讨并提出改进意见\\n2、输出完整对应设计稿与设计规范\\n3、与开发配合，规范开发还原度\\n4、参与产品测试，并跟进项目进度等\",\"end_time\":\"2018年6月\",\"name\":\"星越控股官网（已上线链接www.xingyuekg.com）\",\"describe\":\"合作子公司官网，本人在设计中以黄色为主色，蓝色为辅色，并多处采用高辨识度线性图标与剪影风格背景板，以简洁明快又不失稳重的结构与色彩彰显企业活力与企业精神。\",\"id\":\"BD6D009342A87BBB5CE1624918C87165\",\"sort_id\":2},\"E3E8E30DFD0F00E601F1B69FD58C9B08\":{\"start_time\":\"2017年6月\",\"so_far\":\"N\",\"responsibilities\":\"1、负责收集行业相关数据与竞品数据并进行整合分析\\n2、针对产品功能与交互提出对应专业性意见\\n3、收集产品对应素材，输出设计稿与设计规范\\n4、配合开发相关工作，保证设计稿还原质量\\n5、参与后续版本迭代与部分运营设计等\",\"end_time\":\"2017年10月\",\"name\":\"8kg直播APP（已上线）\",\"describe\":\"公司孵化直播类项目，以多屏直播与主播带货种草为核心功能打造推广化的直播电商新模式。\",\"id\":\"E3E8E30DFD0F00E601F1B69FD58C9B08\",\"sort_id\":3},\"52A77541A549F3A0704E9185A3B05026\":{\"start_time\":\"2017年1月\",\"so_far\":\"N\",\"responsibilities\":\"1、负责整体产品的视觉重构，包括但不止包括排版、配色、ICON、产品动效等\\n2、输出视觉规范，并跟进开发进度\\n3、负责相关运营设计包括banner，海报等\",\"end_time\":\"2017年4月\",\"name\":\"吖咪APP（已上线）\",\"describe\":\"公司收购项目，早期为美食社交应用，后改为to BC的融资平台\",\"id\":\"52A77541A549F3A0704E9185A3B05026\",\"sort_id\":4}},\"language\":[{\"level\":\"英语良好\",\"name\":\"英语\"}],\"training\":{},\"diff\":{\"address\":\"1570524423448\",\"updated_at\":\"1570524423448\"},\"source\":{\"src\":\"2\",\"src_no\":\"765933880\"},\"basic\":{\"reason\":\"\",\"corporation_id\":\"\",\"expect_work_at\":\"1周内\",\"achievement\":\"\",\"expect_industry_name\":\"互联网/电子商务,影视/媒体/艺术/文化传播,\",\"bonus\":\"\",\"is_oversea\":\"N\",\"reporting_to\":\"\",\"salary_month\":\"\",\"expect_city_names\":\"上海,\",\"responsibilities\":\"工作描述：\\n1、负责iOS、Android、PC等设备操作系统的视觉设计以及交互设计\\n2、主导设定整体视觉风格设计，包括动效、插图等\\n3、负责参与设计体验、流程等制定和规范\\n4、分析设计经验、提高团队的设计能力\\n5、完成相关运营设计，动效demo，产品包装等\\n6、与整个部门紧密合作，协同达成最终产品目标。\\n7、完成部门安排的其他工作\",\"basic_salary_origin\":\"15万元\",\"discipline_desc\":\"\",\"basic_salary_to\":\"\",\"account_origin\":\"郑州\",\"id\":\"142693579\",\"station_name\":\"\",\"current_status_origin\":\"目前正在找工作\",\"work_type\":\"\",\"degree\":\"1\",\"birth\":\"1994年\",\"architecture_name\":\"\",\"expect_salary_origin\":\"12000-20000元/月\",\"start_time\":\"2016年8月\",\"so_far\":\"N\",\"management_experience\":\"N\",\"name\":\"徐兵\",\"current_status\":1,\"expect_type\":\"全职\",\"annual_salary_from\":\"\",\"corporation_type\":\"民营公司\",\"name_origin\":\"徐兵\",\"gender_origin\":\"男\",\"expect_salary_to\":\"20.0\",\"expect_position_name\":\"\",\"gender\":\"M\",\"city\":\"\",\"resume_updated_at\":\"2019-10-08 00:00:00\",\"scale\":\"150-500人\",\"corporation_name\":\"上海盛大网络发展有限公司\",\"corporation_desc\":\"\",\"annual_salary_to\":\"\",\"uid\":8544,\"expect_city_ids\":\"105,\",\"updated_at\":\"2019-10-08 00:00:00\",\"self_remark\":\"1、拥有三年设计经验（含pc、app、web、小程序、系统后台等）。\\n2、拥有宽广的行业如交互设计、UI设计、视觉设计、3D设计等视野与时尚的审美标准。\\n3、拥有深厚的设计理论与娴熟的设计技巧，熟练使用axure、sketch、ps、xd、ae、c4d、ai等相关软件，熟悉html5，css3，能够完成静态页面设计。\\n4、热爱设计，交互，对用户体验有深入研究。有较好的逻辑思维能力。具备一定的产品思维和商业思维。\\n5、思维活跃，乐于学习、分析和总结。如总结项目经验，了解最新的设计趋势、跟进业界动态、更新知识储备等。\\n6、积极主动、关注细节、学习能力强，有一定的领导能力。能独立承担项目，有责任感，以结果为导向。\\n7、思维逻辑清晰，写作与表达能力出众。\",\"expect_salary_from\":\"12.0\",\"avatar_mark\":\"\",\"annual_salary\":\"\",\"title_name\":\"\",\"work_experience\":36,\"discipline_name\":\"视觉传达设计\",\"subordinates_count\":\"\",\"industry_name\":\"计算机软件\",\"address\":\"3996\",\"end_time\":\"2019年8月\",\"position_name\":\"UI设计师\",\"school_name\":\"郑州商学院\",\"is_entrance\":\"\",\"address_origin\":\"上海-闵行区\",\"basic_salary_from\":\"\",\"a_p_b\":\"\",\"basic_salary\":\"\",\"account\":\"184\"},\"is_sort\":\"1\"}";
        String resumeId = "2000142693579854600";
        Map<String, Object> compressMap = JSONObject.parseObject(compress);
        try {
            CvDssmHttp cvDssmHttp = new CvDssmHttp(resumeId, compressMap);
            String result = cvDssmHttp.start(cvDssmHttp);
            logger.info("{}", result);
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }

    }

}
