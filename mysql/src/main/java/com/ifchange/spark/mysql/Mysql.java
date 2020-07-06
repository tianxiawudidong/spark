package com.ifchange.spark.mysql;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.util.MyString;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;


/**
 * MYSQL数据库底层封装
 *
 * @author Administrator
 */
public class Mysql implements Serializable {

    private PreparedStatement pstmt;
    private Connection conn;
    private ResultSet rs;
    private volatile boolean isBusy = false;
    private String tableName = "mysql";
    private String dbUsername;
    private String passWord;
    private String dbhost = "127.0.0.1";
    private int dbport = 3306;
    private String enCoding = "UTF-8";
    private static final Logger logger = LoggerFactory.getLogger(Mysql.class);
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public Mysql(String username, String password) throws Exception {
        try {
            dbUsername = username;
            passWord = password;
            createConn();
        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    public Mysql(String username, String password, String dbName) throws Exception {
        try {
            tableName = dbName;
            dbUsername = username;
            passWord = password;
            createConn();
        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    public Mysql(String username, String password, String dbName, String host) throws Exception {
        try {
            tableName = dbName;
            dbUsername = username;
            dbhost = host;
            passWord = password;
            createConn();
        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    public Mysql(String username, String password, String dbName, String host, int port)
        throws Exception {
        try {
            tableName = dbName;
            dbUsername = username;
            dbhost = host;
            dbport = port;
            passWord = password;
            createConn();
        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    public Mysql(String username, String password, String dbName, String host, int port,
                 String encoding) throws Exception {
        try {
            tableName = dbName;
            dbUsername = username;
            dbhost = host;
            dbport = port;
            passWord = password;
            enCoding = encoding;
            createConn();
        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    public String getTableName() {
        return tableName;
    }

    public String getHost() {
        return dbhost;
    }

    private void createConn() throws Exception {
        try {
            conn = DBConnection
                .getDBConnection(dbUsername, passWord, tableName, dbhost, dbport, enCoding);
        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    public boolean updateOrAdd(String[] column, int[] type, String sql) throws SQLException {
        if (!setPstmtParam(column, type, sql)) {
            return false;
        }
        boolean flag = pstmt.executeUpdate() > 0;
        close();
        return flag;
    }

    public DataTable getResultData(String[] coulmn, int[] type, String sql) throws SQLException {
        DataTable dt = new DataTable();

        ArrayList<HashMap<String, String>> list = new ArrayList<>();

        if (!setPstmtParam(coulmn, type, sql)) {
            return null;
        }
        rs = pstmt.executeQuery();
        ResultSetMetaData rsmd = rs.getMetaData();//取数据库的列名
        int numberOfColumns = rsmd.getColumnCount();
        while (rs.next()) {
            HashMap<String, String> rsTree = new HashMap<>();
            for (int r = 1; r < numberOfColumns + 1; r++) {
                rsTree.put(rsmd.getColumnName(r), rs.getObject(r).toString());
            }
            list.add(rsTree);
        }
        close();
        dt.setDataTable(list);
        return dt;
    }

    private boolean setPstmtParam(String[] coulmn, int[] type, String sql)
        throws NumberFormatException, SQLException {
        if (sql == null) {
            return false;
        }
        pstmt = conn.prepareStatement(sql);
        if (coulmn != null && type != null && coulmn.length != 0 && type.length != 0) {
            for (int i = 0; i < type.length; i++) {
                switch (type[i]) {
                    case Types.INTEGER:
                        pstmt.setInt(i + 1, Integer.parseInt(coulmn[i]));
                        break;
                    case Types.SMALLINT:
                        pstmt.setInt(i + 1, Integer.parseInt(coulmn[i]));
                        break;
                    case Types.BOOLEAN:
                        pstmt.setBoolean(i + 1, Boolean.parseBoolean(coulmn[i]));
                        break;
                    case Types.CHAR:
                        pstmt.setString(i + 1, coulmn[i]);
                        break;
                    case Types.DOUBLE:
                        pstmt.setDouble(i + 1, Double.parseDouble(coulmn[i]));
                        break;
                    case Types.FLOAT:
                        pstmt.setFloat(i + 1, Float.parseFloat(coulmn[i]));
                        break;
                    case Types.BIGINT:
                        pstmt.setLong(i + 1, Long.parseLong(coulmn[i]));
                        break;
                    default:
                        break;
                }
            }
        }
        return true;
    }

    public void close() throws SQLException {
        if (rs != null) {
            rs.close();
            rs = null;
        }
        if (pstmt != null) {
            pstmt.close();
            pstmt = null;
        }
        if (conn != null) {
            conn.close();
        }
    }

    public List<Map<String, Object>> executeQuery(String sql) throws SQLException {
        busy();
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql);
                rs = pstmt.executeQuery();
                ResultSetMetaData rsmd = rs.getMetaData();//取数据库的列名
                int numberOfColumns = rsmd.getColumnCount();
                while (rs.next()) {
                    HashMap<String, Object> rsTree = new HashMap<>();
                    for (int r = 1; r < numberOfColumns + 1; r++) {
                        rsTree.put(rsmd.getColumnLabel(r), rs.getObject(r));
                    }
                    list.add(rsTree);
                }
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
        return list;
    }

    public List<Map<String, String>> queryResumesMaps(String sql) throws SQLException {
        busy();
        ArrayList<Map<String, String>> list = new ArrayList<>();
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql);
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    HashMap<String, String> map = new HashMap<>();
                    map.put("id", rs.getString("id"));
                    map.put("resume_id", rs.getString("resume_id"));
                    map.put("src", rs.getString("src"));
                    map.put("src_no", rs.getString("src_no"));
                    map.put("show_src", rs.getString("show_src"));
                    map.put("show_src_no", rs.getString("show_src_no"));
                    map.put("is_deleted", rs.getString("is_deleted"));
                    Timestamp updatedAt = rs.getTimestamp("updated_at");
                    String update = "";
                    if (null != updatedAt) {
                        LocalDateTime localDateTime = updatedAt.toLocalDateTime();
                        update = dtf.format(localDateTime);
                    }
                    map.put("updated_at", update);
                    Timestamp createdAt = rs.getTimestamp("created_at");
                    String create = "";
                    if (null != createdAt) {
                        LocalDateTime localDateTime = createdAt.toLocalDateTime();
                        create = dtf.format(localDateTime);
                    }
                    map.put("created_at", create);
                    list.add(map);
                }
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
        return list;
    }

    public List<Map<String, String>> queryPositionMaps(String sql) throws SQLException {
        busy();
        List<Map<String, String>> list = new ArrayList<>();
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql);
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    Map<String, String> rsTree = new HashMap<>();
                    rsTree.put("id", rs.getString("id"));
                    rsTree.put("position_id", rs.getString("position_id"));
                    rsTree.put("source", rs.getString("source"));
                    rsTree.put("source_id", rs.getString("source_id"));
                    rsTree.put("is_deleted", rs.getString("is_deleted"));
                    Timestamp updatedAt = rs.getTimestamp("updated_at");
                    String update = "";
                    if (null != updatedAt) {
                        LocalDateTime localDateTime = updatedAt.toLocalDateTime();
                        update = dtf.format(localDateTime);
                    }
                    rsTree.put("updated_at", update);
                    Timestamp createdAt = rs.getTimestamp("created_at");
                    String create = "";
                    if (null != createdAt) {
                        LocalDateTime localDateTime = createdAt.toLocalDateTime();
                        create = dtf.format(localDateTime);
                    }
                    rsTree.put("created_at", create);
                    list.add(rsTree);
                }
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
        return list;
    }

    public Map<String, Object> queryPositions(String sql) throws SQLException {
        busy();
        Map<String, Object> result = new HashMap<>();
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();//取数据库的列名
            int numberOfColumns = rsmd.getColumnCount();
            while (rs.next()) {
                for (int r = 1; r < numberOfColumns + 1; r++) {
                    result.put(rsmd.getColumnLabel(r), rs.getString(r));
                }
            }
            free();
        } catch (SQLException ex) {
            processException(ex);
        }
        return result;
    }

    public Map<String, Long> getResumes(String sql) throws SQLException {
        busy();
        Map<String, Long> result = new HashMap<>();
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                long id = rs.getLong("id");
                long contactId = rs.getLong("contact_id");
                result.put("id", id);
                result.put("contact_id", contactId);
            }
            free();
        } catch (SQLException ex) {
            processException(ex);
        }
        return result;
    }

    public Map<String, Object> getUpdateDeliver(String sql) throws SQLException {
        busy();
        Map<String, Object> result = new HashMap<>();
        int deliverNum = 0;
        int updateNum = 0;
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql);
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    deliverNum =
                        null != rs.getObject("days7_deliver_num") ? rs.getInt("days7_deliver_num")
                            : 0;
                    updateNum =
                        null != rs.getObject("days7_update_num") ? rs.getInt("days7_update_num")
                            : 0;
                }
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
        result.put("times_deliver", deliverNum);
        result.put("times_update", updateNum);
        return result;
    }


    //MariaDB [icdc_40]> desc resumes_extras;
    //+--------------+---------------------+------+-----+-------------------+-----------------------------+
    //| Field        | Type                | Null | Key | Default           | Extra                       |
    //+--------------+---------------------+------+-----+-------------------+-----------------------------+
    //| id           | bigint(20) unsigned | NO   | PRI | 0                 |                             |
    //| compress     | longblob            | NO   |     | NULL              |                             |
    //| cv_source    | text                | NO   |     | NULL              |                             |
    //| cv_trade     | text                | NO   |     | NULL              |                             |
    //| cv_title     | text                | NO   |     | NULL              |                             |
    //| cv_tag       | text                | NO   |     | NULL              |                             |
    //| skill_tag    | text                | NO   |     | NULL              |                             |
    //| personal_tag | text                | NO   |     | NULL              |                             |
    //| updated_at   | timestamp           | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
    //| created_at   | timestamp           | NO   |     | CURRENT_TIMESTAMP |                             |
    //+--------------+---------------------+------+-----+-------------------+-----------------------------+
    public List<Map<String, String>> getResumeExtra(String sql) throws SQLException {
        List<Map<String, String>> list = new ArrayList<>();
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                Map<String, String> result = new HashMap<>();
                String id = String.valueOf(rs.getLong("id"));
                String compress = new String(rs.getBytes("compress"), StandardCharsets.UTF_8);
                String cvSource = rs.getString("cv_source");
                String cvTrade = rs.getString("cv_trade");
                String cvTitle = rs.getString("cv_title");
                String cvTag = rs.getString("cv_tag");
                String skillTag = rs.getString("skill_tag");
                String personalTag = rs.getString("personal_tag");
                String createdAt = "";
                Timestamp create = null != rs.getObject("created_at") ? rs.getTimestamp("created_at") : null;
                if (null != create) {
                    LocalDateTime localDateTime = create.toLocalDateTime();
                    createdAt = localDateTime.format(dtf);
                }
                String updatedAt = "";
                Timestamp update = null != rs.getObject("updated_at") ? rs.getTimestamp("updated_at") : null;
                if (null != update) {
                    LocalDateTime localDateTime = update.toLocalDateTime();
                    updatedAt = localDateTime.format(dtf);
                }
                result.put("id", id);
                result.put("compress", compress);
                result.put("cv_source", cvSource);
                result.put("cv_trade", cvTrade);
                result.put("cv_title", cvTitle);
                result.put("cv_tag", cvTag);
                result.put("skill_tag", skillTag);
                result.put("personal_tag", personalTag);
                result.put("created_at", createdAt);
                result.put("updated_at", updatedAt);
                list.add(result);
            }
            free();
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return list;
    }


    public List<Map<String, String>> getTobResumeDetail(String sql) throws SQLException {
        List<Map<String, String>> list = new ArrayList<>();
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                Map<String, String> result = new HashMap<>();
                String tobResumeId = String.valueOf(rs.getLong("tob_resume_id"));
                String resumeContent = rs.getString("resume_content");
                String createdAt = "";
                Timestamp create = null != rs.getObject("created_at") ? rs.getTimestamp("created_at") : null;
                if (null != create) {
                    LocalDateTime localDateTime = create.toLocalDateTime();
                    createdAt = localDateTime.format(dtf);
                }
                String updatedAt = "";
                Timestamp update = null != rs.getObject("updated_at") ? rs.getTimestamp("updated_at") : null;
                if (null != update) {
                    LocalDateTime localDateTime = update.toLocalDateTime();
                    updatedAt = localDateTime.format(dtf);
                }

                result.put("tob_resume_id", tobResumeId);
                result.put("resume_content", resumeContent);
                result.put("created_at", createdAt);
                result.put("updated_at", updatedAt);
                list.add(result);
            }
            free();
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return list;
    }

    public List<Map<String, String>> getTobAtsDelivery(String sql) throws SQLException {
        List<Map<String, String>> list = new ArrayList<>();
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                Map<String, String> result = new HashMap<>();
                String deliveryId = String.valueOf(rs.getLong("delivery_id"));
                String tobPositionId = String.valueOf(rs.getLong("tob_position_id"));
                String icdcPositionId = String.valueOf(rs.getLong("icdc_position_id"));
                String tobResumeId = String.valueOf(rs.getLong("tob_resume_id"));
                String createdAt = "";
                Timestamp create = null != rs.getObject("created_at") ? rs.getTimestamp("created_at") : null;
                if (null != create) {
                    LocalDateTime localDateTime = create.toLocalDateTime();
                    createdAt = localDateTime.format(dtf);
                }
                String updatedAt = "";
                Timestamp update = null != rs.getObject("updated_at") ? rs.getTimestamp("updated_at") : null;
                if (null != update) {
                    LocalDateTime localDateTime = update.toLocalDateTime();
                    updatedAt = localDateTime.format(dtf);
                }

                String isDeleted = String.valueOf(rs.getInt("is_deleted"));
                result.put("delivery_id", deliveryId);
                result.put("tob_position_id", tobPositionId);
                result.put("icdc_position_id", icdcPositionId);
                result.put("tob_resume_id", tobResumeId);
                result.put("created_at", createdAt);
                result.put("updated_at", updatedAt);
                result.put("is_deleted", isDeleted);
                list.add(result);
            }
            free();
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return list;
    }

    public Map<String, String> getTobAtsRecruitStep(String sql) throws SQLException {
        Map<String, String> result = new HashMap<>();
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String stepId = String.valueOf(rs.getLong("step_id"));
                String uid = String.valueOf(rs.getLong("uid"));
                String topId = String.valueOf(rs.getLong("top_id"));
                String tobPositionId = String.valueOf(rs.getLong("tob_position_id"));
                String resumeId = String.valueOf(rs.getLong("resume_id"));
                String tobResumeId = String.valueOf(rs.getLong("tob_resume_id"));
                String deliverSource = String.valueOf(rs.getInt("deliver_source"));
                String stepType = String.valueOf(rs.getInt("step_type"));
                String stepStatus = String.valueOf(rs.getInt("step_status"));
                String remark = rs.getString("remark");
                String createdAt = "";
                Timestamp create = null != rs.getObject("created_at") ? rs.getTimestamp("created_at") : null;
                if (null != create) {
                    LocalDateTime localDateTime = create.toLocalDateTime();
                    createdAt = localDateTime.format(dtf);
                }
                String updatedAt = "";
                Timestamp update = null != rs.getObject("updated_at") ? rs.getTimestamp("updated_at") : null;
                if (null != update) {
                    LocalDateTime localDateTime = update.toLocalDateTime();
                    updatedAt = localDateTime.format(dtf);
                }
                String transfer_stage_at = "";
                Timestamp transfer = null != rs.getObject("transfer_stage_at") ?
                    rs.getTimestamp("transfer_stage_at") : null;
                if (null != transfer) {
                    LocalDateTime localDateTime = transfer.toLocalDateTime();
                    transfer_stage_at = localDateTime.format(dtf);
                }
                String isDeleted = String.valueOf(rs.getInt("is_deleted"));
                String stageId = String.valueOf(rs.getLong("stage_id"));
                String stageIdFrom = String.valueOf(rs.getLong("stage_id_from"));
                String stageTypeId = String.valueOf(rs.getLong("stage_type_id"));
                String hrStatus = String.valueOf(rs.getInt("hr_status"));
                String sourceId = String.valueOf(rs.getLong("source_id"));
                String stageTypeFrom = String.valueOf(rs.getInt("stage_type_from"));

                result.put("step_id", stepId);
                result.put("uid", uid);
                result.put("top_id", topId);
                result.put("tob_position_id", tobPositionId);
                result.put("resume_id", resumeId);
                result.put("tob_resume_id", tobResumeId);
                result.put("deliver_source", deliverSource);
                result.put("step_type", stepType);
                result.put("step_status", stepStatus);
                result.put("remark", remark);
                result.put("updated_at", updatedAt);
                result.put("created_at", createdAt);
                result.put("transfer_stage_at", transfer_stage_at);
                result.put("is_deleted", isDeleted);
                result.put("stage_id", stageId);
                result.put("stage_id_from", stageIdFrom);
                result.put("stage_type_id", stageTypeId);
                result.put("hr_status", hrStatus);
                result.put("source_id", sourceId);
                result.put("stage_type_from", stageTypeFrom);
            }
            free();
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return result;
    }

    public String getTobResumeDetailContent(String sql) throws SQLException {
        String content = "";
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String resumeContent = rs.getString("resume_content");
                if (StringUtils.isNoneBlank(resumeContent)) {
                    content = new String(MyString.gzipUncompress(Base64.getDecoder().decode(resumeContent)));
                }
            }
            free();
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return content;
    }


    public Map<String, String> getTobResumesUpdate(String sql) throws SQLException {
        Map<String, String> result = new HashMap<>();
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String tobResumeId = String.valueOf(rs.getLong("tob_resume_id"));
                Timestamp updatedAt = null != rs.getObject("updated_at") ?
                    rs.getTimestamp("updated_at") : null;
                String update = "";
                if (null != updatedAt) {
                    LocalDateTime localDateTime = updatedAt.toLocalDateTime();
                    update = dtf.format(localDateTime);
                }
                result.put("tob_resume_id", tobResumeId);
                result.put("updated_at", update);
            }
            free();
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return result;
    }

    public String getUpdatedAt(String sql) throws SQLException {
        String updated = "";
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                Timestamp updatedAt = null != rs.getObject("updated_at") ?
                    rs.getTimestamp("updated_at") : null;
                if (null != updatedAt) {
                    LocalDateTime localDateTime = updatedAt.toLocalDateTime();
                    updated = dtf.format(localDateTime);
                }
            }
            free();
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return updated;
    }


    public List<Map<String, String>> getTobAlgorithms(String sql) throws SQLException {
        List<Map<String, String>> list = new ArrayList<>();
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                Map<String, String> result = new HashMap<>();
                String tobResumeId = String.valueOf(rs.getLong("tob_resume_id"));
                String data = new String(rs.getBytes("data"), StandardCharsets.UTF_8);
                String createdAt = "";
                Timestamp create = null != rs.getObject("created_at") ? rs.getTimestamp("created_at") : null;
                if (null != create) {
                    LocalDateTime localDateTime = create.toLocalDateTime();
                    createdAt = localDateTime.format(dtf);
                }
                String updatedAt = "";
                Timestamp update = null != rs.getObject("updated_at") ? rs.getTimestamp("updated_at") : null;
                if (null != update) {
                    LocalDateTime localDateTime = update.toLocalDateTime();
                    updatedAt = localDateTime.format(dtf);
                }
                result.put("tob_resume_id", tobResumeId);
                result.put("data", data);
                result.put("created_at", createdAt);
                result.put("updated_at", updatedAt);
                list.add(result);
            }
            free();
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return list;
    }

    public Map<String, String> getAlgorithms(String sql) throws SQLException {
        Map<String, String> result = new HashMap<>();
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String id = String.valueOf(rs.getLong("id"));
                String data = new String(rs.getBytes("data"), StandardCharsets.UTF_8);
                String createdAt = "";
                Timestamp create = null != rs.getObject("created_at") ? rs.getTimestamp("created_at") : null;
                if (null != create) {
                    LocalDateTime localDateTime = create.toLocalDateTime();
                    createdAt = localDateTime.format(dtf);
                }
                String updatedAt = "";
                Timestamp update = null != rs.getObject("updated_at") ? rs.getTimestamp("updated_at") : null;
                if (null != update) {
                    LocalDateTime localDateTime = update.toLocalDateTime();
                    updatedAt = localDateTime.format(dtf);
                }
                result.put("id", id);
                result.put("data", data);
                result.put("created_at", createdAt);
                result.put("updated_at", updatedAt);
            }
            free();
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return result;
    }

    public List<Map<String, String>> getAlgorithmsList(String sql) throws SQLException {
        List<Map<String, String>> list = new ArrayList<>();
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                Map<String, String> result = new HashMap<>();
                String id = String.valueOf(rs.getLong("id"));
                String data = new String(rs.getBytes("data"), StandardCharsets.UTF_8);
                String createdAt = "";
                Timestamp create = null != rs.getObject("created_at") ? rs.getTimestamp("created_at") : null;
                if (null != create) {
                    LocalDateTime localDateTime = create.toLocalDateTime();
                    createdAt = localDateTime.format(dtf);
                }
                String updatedAt = "";
                Timestamp update = null != rs.getObject("updated_at") ? rs.getTimestamp("updated_at") : null;
                if (null != update) {
                    LocalDateTime localDateTime = update.toLocalDateTime();
                    updatedAt = localDateTime.format(dtf);
                }
                result.put("id", id);
                result.put("data", data);
                result.put("created_at", createdAt);
                result.put("updated_at", updatedAt);
                list.add(result);
            }
            free();
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return list;
    }

    public List<Map<String, String>> getPositionsList(String sql) throws SQLException {
        List<Map<String, String>> list = new ArrayList<>();
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                Map<String, String> result = new HashMap<>();
                String id = String.valueOf(rs.getLong("id"));
                String corporationId = String.valueOf(rs.getLong("corporation_id"));
                String corporationName = rs.getString("corporation_name");
                String name = rs.getString("name");
                String cityIds = rs.getString("city_ids");
                String architectureName = rs.getString("architecture_name");
                String salaryBegin = String.valueOf(rs.getLong("salary_begin"));
                String salaryEnd = String.valueOf(rs.getLong("salary_end"));
                String dailySalaryBegin = String.valueOf(rs.getLong("daily_salary_begin"));
                String dailySalaryEnd = String.valueOf(rs.getLong("daily_salary_end"));
                String annualSalaryBegin = String.valueOf(rs.getLong("annual_salary_begin"));
                String annualSalaryEnd = String.valueOf(rs.getLong("annual_salary_end"));
                String experienceBegin = String.valueOf(rs.getInt("experience_begin"));
                String experienceEnd = String.valueOf(rs.getInt("experience_end"));
                String degreeId = String.valueOf(rs.getInt("degree_id"));
                String degreeIsUp = String.valueOf(rs.getInt("degree_is_up"));
                String userId = String.valueOf(rs.getLong("user_id"));
                String topId = String.valueOf(rs.getLong("top_id"));
                String status = String.valueOf(rs.getInt("status"));
                String isShow = rs.getString("is_show");
                String isSource = rs.getString("is_source");
                String isShort = String.valueOf(rs.getInt("is_short"));
                String isAiSite = String.valueOf(rs.getInt("is_ai_site"));
                String isDeleted = rs.getString("is_deleted");
                String createdAt = "";
                Timestamp create = null != rs.getObject("created_at") ? rs.getTimestamp("created_at") : null;
                if (null != create) {
                    LocalDateTime localDateTime = create.toLocalDateTime();
                    createdAt = localDateTime.format(dtf);
                }
                String updatedAt = "";
                Timestamp update = null != rs.getObject("updated_at") ? rs.getTimestamp("updated_at") : null;
                if (null != update) {
                    LocalDateTime localDateTime = update.toLocalDateTime();
                    updatedAt = localDateTime.format(dtf);
                }
                String refreshedAt = "";
                Timestamp refreshed = null != rs.getObject("refreshed_at") ? rs.getTimestamp("refreshed_at") : null;
                if (null != refreshed) {
                    LocalDateTime localDateTime = refreshed.toLocalDateTime();
                    refreshedAt = localDateTime.format(dtf);
                }
                String editedAt = "";
                Timestamp edited = null != rs.getObject("edited_at") ? rs.getTimestamp("edited_at") : null;
                if (null != edited) {
                    LocalDateTime localDateTime = edited.toLocalDateTime();
                    editedAt = localDateTime.format(dtf);
                }
                String grabDate = "";
                Timestamp grab = null != rs.getObject("grab_date") ? rs.getTimestamp("grab_date") : null;
                if (null != grab) {
                    LocalDateTime localDateTime = grab.toLocalDateTime();
                    grabDate = localDateTime.format(dtf);
                }
                String lastViewTime = "";
                Timestamp last_view = null != rs.getObject("last_view_time") ? rs.getTimestamp("last_view_time") : null;
                if (null != last_view) {
                    LocalDateTime localDateTime = last_view.toLocalDateTime();
                    lastViewTime = localDateTime.format(dtf);
                }

                result.put("id", id);
                result.put("corporation_id", corporationId);
                result.put("corporation_name", corporationName);
                result.put("name", name);
                result.put("city_ids", cityIds);
                result.put("architecture_name", architectureName);
                result.put("salary_begin", salaryBegin);
                result.put("salary_end", salaryEnd);
                result.put("daily_salary_begin", dailySalaryBegin);
                result.put("daily_salary_end", dailySalaryEnd);
                result.put("annual_salary_begin", annualSalaryBegin);
                result.put("annual_salary_end", annualSalaryEnd);
                result.put("experience_begin", experienceBegin);
                result.put("experience_end", experienceEnd);
                result.put("degree_id", degreeId);
                result.put("degree_is_up", degreeIsUp);
                result.put("user_id", userId);
                result.put("top_id", topId);
                result.put("status", status);
                result.put("is_show", isShow);
                result.put("is_source", isSource);
                result.put("is_short", isShort);
                result.put("is_ai_site", isAiSite);
                result.put("is_deleted", isDeleted);
                result.put("created_at", createdAt);
                result.put("updated_at", updatedAt);
                result.put("refreshed_at", refreshedAt);
                result.put("edited_at", editedAt);
                result.put("grab_date", grabDate);
                result.put("last_view_time", lastViewTime);
                list.add(result);
            }
            free();
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return list;
    }

    //| id                       | bigint(20) unsigned | NO   | PRI | NULL                |       |
    //| jd_functions             | text                | NO   |     | NULL                |       |
    //| jd_schools               | text                | NO   |     | NULL                |       |
    //| jd_corporations          | text                | NO   |     | NULL                |       |
    //| jd_original_corporations | text                | NO   |     | NULL                |       |
    //| jd_features              | text                | NO   |     | NULL                |       |
    //| jd_tags                  | text                | NO   |     | NULL                |       |
    //| jd_trades                | text                | NO   |     | NULL                |       |
    //| jd_titles                | text                | NO   |     | NULL                |       |
    //| jd_entities              | text                | NO   |     | NULL                |       |
    //| jd_address               | text                | NO   |     | NULL                |       |
    //| jd_other                 | varchar(256)        | NO   |     |                     |       |
    //| jd_real_corporations     | text                | NO   |     | NULL                |       |
    //| jd_comment               | text                | YES  |     | NULL                |       |
    //| jd_ner_skill             | text                | NO   |     | NULL                |       |
    //| human_tags               | varchar(256)        | NO   |     |                     |       |
    //| is_deleted               | enum('Y','N')       | NO   |     | N                   |       |
    //| created_at               | timestamp           | NO   |     | CURRENT_TIMESTAMP   |       |
    //| updated_at               | timestamp           | NO   |     | 0000-00-00 00:00:00 |
    public Map<String, String> getPositionsAlgorithms(String sql) throws SQLException {
        Map<String, String> result = new HashMap<>();
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String id = String.valueOf(rs.getLong("id"));
                String jd_functions = rs.getString("jd_functions");
                String jd_schools = rs.getString("jd_schools");
                String jd_corporations = rs.getString("jd_corporations");
                String jd_original_corporations = rs.getString("jd_original_corporations");
                String jd_features = rs.getString("jd_features");
                String jd_tags = rs.getString("jd_tags");
                String jd_trades = rs.getString("jd_trades");
                String jd_titles = rs.getString("jd_titles");
                String jd_entities = rs.getString("jd_entities");
                String jd_address = rs.getString("jd_address");
                String jd_other = rs.getString("jd_other");
                String jd_real_corporations = rs.getString("jd_real_corporations");
                String jd_comment = rs.getString("jd_comment");
                String jd_ner_skill = rs.getString("jd_ner_skill");
                String human_tags = rs.getString("human_tags");
                String is_deleted = rs.getString("is_deleted");
                String createdAt = "";
                Timestamp create = null != rs.getObject("created_at") ? rs.getTimestamp("created_at") : null;
                if (null != create) {
                    LocalDateTime localDateTime = create.toLocalDateTime();
                    createdAt = localDateTime.format(dtf);
                }
                String updatedAt = "";
                Timestamp update = null != rs.getObject("updated_at") ? rs.getTimestamp("updated_at") : null;
                if (null != update) {
                    LocalDateTime localDateTime = update.toLocalDateTime();
                    updatedAt = localDateTime.format(dtf);
                }

                result.put("id", id);
                result.put("jd_functions", jd_functions);
                result.put("jd_schools", jd_schools);
                result.put("jd_corporations", jd_corporations);
                result.put("jd_original_corporations", jd_original_corporations);
                result.put("jd_features", jd_features);
                result.put("jd_tags", jd_tags);
                result.put("jd_trades", jd_trades);
                result.put("jd_titles", jd_titles);
                result.put("jd_entities", jd_entities);
                result.put("jd_address", jd_address);
                result.put("jd_other", jd_other);
                result.put("jd_real_corporations", jd_real_corporations);
                result.put("jd_comment", jd_comment);
                result.put("jd_ner_skill", jd_ner_skill);
                result.put("human_tags", human_tags);
                result.put("is_deleted", is_deleted);
                result.put("created_at", createdAt);
                result.put("updated_at", updatedAt);
            }
            free();
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return result;
    }

    public List<Map<String, String>> getPositionsAlgorithmsList(String sql) throws SQLException {
        List<Map<String, String>> list = new ArrayList<>();
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                Map<String, String> result = new HashMap<>();
                String id = String.valueOf(rs.getLong("id"));
                String jd_functions = rs.getString("jd_functions");
                String jd_schools = rs.getString("jd_schools");
                String jd_corporations = rs.getString("jd_corporations");
                String jd_original_corporations = rs.getString("jd_original_corporations");
                String jd_features = rs.getString("jd_features");
                String jd_tags = rs.getString("jd_tags");
                String jd_trades = rs.getString("jd_trades");
                String jd_titles = rs.getString("jd_titles");
                String jd_entities = rs.getString("jd_entities");
                String jd_address = rs.getString("jd_address");
                String jd_other = rs.getString("jd_other");
                String jd_real_corporations = rs.getString("jd_real_corporations");
                String jd_comment = rs.getString("jd_comment");
                String jd_ner_skill = rs.getString("jd_ner_skill");
                String human_tags = rs.getString("human_tags");
                String is_deleted = rs.getString("is_deleted");
                String createdAt = "";
                Timestamp create = null != rs.getObject("created_at") ? rs.getTimestamp("created_at") : null;
                if (null != create) {
                    LocalDateTime localDateTime = create.toLocalDateTime();
                    createdAt = localDateTime.format(dtf);
                }
                String updatedAt = "";
                Timestamp update = null != rs.getObject("updated_at") ? rs.getTimestamp("updated_at") : null;
                if (null != update) {
                    LocalDateTime localDateTime = update.toLocalDateTime();
                    updatedAt = localDateTime.format(dtf);
                }

                result.put("id", id);
                result.put("jd_functions", jd_functions);
                result.put("jd_schools", jd_schools);
                result.put("jd_corporations", jd_corporations);
                result.put("jd_original_corporations", jd_original_corporations);
                result.put("jd_features", jd_features);
                result.put("jd_tags", jd_tags);
                result.put("jd_trades", jd_trades);
                result.put("jd_titles", jd_titles);
                result.put("jd_entities", jd_entities);
                result.put("jd_address", jd_address);
                result.put("jd_other", jd_other);
                result.put("jd_real_corporations", jd_real_corporations);
                result.put("jd_comment", jd_comment);
                result.put("jd_ner_skill", jd_ner_skill);
                result.put("human_tags", human_tags);
                result.put("is_deleted", is_deleted);
                result.put("created_at", createdAt);
                result.put("updated_at", updatedAt);
                list.add(result);
            }
            free();
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return list;
    }

    public Map<String, String> getPositionsExtras(String sql) throws SQLException {
        Map<String, String> result = new HashMap<>();
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String id = String.valueOf(rs.getLong("id"));
                String email = rs.getString("email");
                String strategy_type = String.valueOf(rs.getInt("strategy_type"));
                String position_type = String.valueOf(rs.getInt("position_type"));
                String shop_id = String.valueOf(rs.getInt("shop_id"));
                String relation_id = String.valueOf(rs.getLong("relation_id"));
                String category = rs.getString("category");
                String hunter_industry = String.valueOf(rs.getLong("hunter_industry"));
                String hunter_suspended = String.valueOf(rs.getLong("hunter_suspended"));
                String hunter_protection_period = String.valueOf(rs.getLong("hunter_protection_period"));
                String hunter_pay_end = String.valueOf(rs.getLong("hunter_pay_end"));
                String hunter_pay_begin = String.valueOf(rs.getLong("hunter_pay_begin"));
                String hunter_salary_end = String.valueOf(rs.getLong("hunter_salary_end"));
                String hunter_salary_begin = String.valueOf(rs.getLong("hunter_salary_begin"));
                String category_id = String.valueOf(rs.getLong("category_id"));
                String real_corporation_name = rs.getString("real_corporation_name");
                String address = rs.getString("address");
                String salary = rs.getString("salary");
                String source_ids = rs.getString("source_ids");
                String project_ids = rs.getString("project_ids");
                String languages = rs.getString("languages");
                String is_oversea = rs.getString("is_oversea");
                String recruit_type = String.valueOf(rs.getInt("recruit_type"));
                String nature = String.valueOf(rs.getInt("nature"));
                String is_inside = String.valueOf(rs.getInt("is_inside"));
                String is_secret = String.valueOf(rs.getInt("is_secret"));
                String number = String.valueOf(rs.getInt("number"));
                String gender = String.valueOf(rs.getInt("gender"));
                String profession = rs.getString("profession");
                String is_pa_researched = String.valueOf(rs.getInt("is_pa_researched"));
                String recommand_resume_count = String.valueOf(rs.getInt("recommand_resume_count"));
                String referral_reward = rs.getString("referral_reward");
                String occupation_commercial_activitie = rs.getString("occupation_commercial_activitie");
                String company_commercial_activitie = rs.getString("company_commercial_activitie");
                String is_manager = rs.getString("is_manager");
                String subordinate = String.valueOf(rs.getInt("subordinate"));
                String manager_years = String.valueOf(rs.getInt("manager_years"));
                String tags = rs.getString("tags");
                String description = rs.getString("description");
                String requirement = rs.getString("requirement");
                String department_desc = rs.getString("department_desc");
                String additional_desc = rs.getString("additional_desc");
                String processing_rate = rs.getString("processing_rate");
                String show_count = rs.getString("show_count");
                String last_source = rs.getString("last_source");
                String last_source_id = rs.getString("last_source_id");
                String refreshed_info = rs.getString("refreshed_info");
                String user_ids = rs.getString("user_ids");
                String organization = rs.getString("organization");
                String hp_research = rs.getString("hp_research");
                String special_period = rs.getString("special_period");
                String pa_info = rs.getString("pa_info");
                String custom_data = rs.getString("custom_data");
                String is_urgent = rs.getString("is_urgent");
                String pa_urgent_level = rs.getString("pa_urgent_level");
                String is_headhunter = rs.getString("is_headhunter");
                String is_headhunter_trade = rs.getString("is_headhunter_trade");
                String is_interpolate = rs.getString("is_interpolate");
                String is_hot = rs.getString("is_hot");
                String is_deleted = rs.getString("is_deleted");
                String createdAt = "";
                Timestamp create = null != rs.getObject("created_at") ? rs.getTimestamp("created_at") : null;
                if (null != create) {
                    LocalDateTime localDateTime = create.toLocalDateTime();
                    createdAt = localDateTime.format(dtf);
                }
                String updatedAt = "";
                Timestamp update = null != rs.getObject("updated_at") ? rs.getTimestamp("updated_at") : null;
                if (null != update) {
                    LocalDateTime localDateTime = update.toLocalDateTime();
                    updatedAt = localDateTime.format(dtf);
                }
                String pa_refreshed_at = "";
                Timestamp pa_refreshed = null != rs.getObject("pa_refreshed_at") ? rs.getTimestamp("pa_refreshed_at") : null;
                if (null != pa_refreshed) {
                    LocalDateTime localDateTime = pa_refreshed.toLocalDateTime();
                    pa_refreshed_at = localDateTime.format(dtf);
                }
                result.put("id", id);
                result.put("email", email);
                result.put("strategy_type", strategy_type);
                result.put("position_type", position_type);
                result.put("shop_id", shop_id);
                result.put("relation_id", relation_id);
                result.put("category", category);
                result.put("hunter_industry", hunter_industry);
                result.put("hunter_suspended", hunter_suspended);
                result.put("hunter_protection_period", hunter_protection_period);
                result.put("hunter_pay_end", hunter_pay_end);
                result.put("hunter_pay_begin", hunter_pay_begin);
                result.put("hunter_salary_end", hunter_salary_end);
                result.put("hunter_salary_begin", hunter_salary_begin);
                result.put("category_id", category_id);
                result.put("real_corporation_name", real_corporation_name);
                result.put("address", address);
                result.put("salary", salary);
                result.put("source_ids", source_ids);
                result.put("project_ids", project_ids);
                result.put("languages", languages);
                result.put("is_oversea", is_oversea);
                result.put("recruit_type", recruit_type);
                result.put("nature", nature);
                result.put("is_inside", is_inside);
                result.put("is_secret", is_secret);
                result.put("number", number);
                result.put("gender", gender);
                result.put("profession", profession);
                result.put("is_pa_researched", is_pa_researched);
                result.put("recommand_resume_count", recommand_resume_count);
                result.put("referral_reward", referral_reward);
                result.put("occupation_commercial_activitie", occupation_commercial_activitie);
                result.put("company_commercial_activitie", company_commercial_activitie);
                result.put("is_manager", is_manager);
                result.put("subordinate", subordinate);
                result.put("manager_years", manager_years);
                result.put("tags", tags);
                result.put("description", description);
                result.put("requirement", requirement);
                result.put("department_desc", department_desc);
                result.put("additional_desc", additional_desc);
                result.put("processing_rate", processing_rate);
                result.put("show_count", show_count);
                result.put("last_source", last_source);
                result.put("last_source_id", last_source_id);
                result.put("refreshed_info", refreshed_info);
                result.put("user_ids", user_ids);
                result.put("organization", organization);
                result.put("hp_research", hp_research);
                result.put("special_period", special_period);
                result.put("pa_info", pa_info);
                result.put("custom_data", custom_data);
                result.put("is_urgent", is_urgent);
                result.put("pa_urgent_level", pa_urgent_level);
                result.put("is_headhunter", is_headhunter);
                result.put("is_headhunter_trade", is_headhunter_trade);
                result.put("is_interpolate", is_interpolate);
                result.put("is_hot", is_hot);
                result.put("is_deleted", is_deleted);
                result.put("created_at", createdAt);
                result.put("updated_at", updatedAt);
                result.put("pa_refreshed_at", pa_refreshed_at);
            }
            free();
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return result;
    }

    public List<Map<String, String>> getPositionsExtrasList(String sql) throws SQLException {
        List<Map<String, String>> list = new ArrayList<>();
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                Map<String, String> result = new HashMap<>();
                String id = String.valueOf(rs.getLong("id"));
                String email = rs.getString("email");
                String strategy_type = String.valueOf(rs.getInt("strategy_type"));
                String position_type = String.valueOf(rs.getInt("position_type"));
                String shop_id = String.valueOf(rs.getInt("shop_id"));
                String relation_id = String.valueOf(rs.getLong("relation_id"));
                String category = rs.getString("category");
                String hunter_industry = String.valueOf(rs.getLong("hunter_industry"));
                String hunter_suspended = String.valueOf(rs.getLong("hunter_suspended"));
                String hunter_protection_period = String.valueOf(rs.getLong("hunter_protection_period"));
                String hunter_pay_end = String.valueOf(rs.getLong("hunter_pay_end"));
                String hunter_pay_begin = String.valueOf(rs.getLong("hunter_pay_begin"));
                String hunter_salary_end = String.valueOf(rs.getLong("hunter_salary_end"));
                String hunter_salary_begin = String.valueOf(rs.getLong("hunter_salary_begin"));
                String category_id = String.valueOf(rs.getLong("category_id"));
                String real_corporation_name = rs.getString("real_corporation_name");
                String address = rs.getString("address");
                String salary = rs.getString("salary");
                String source_ids = rs.getString("source_ids");
                String project_ids = rs.getString("project_ids");
                String languages = rs.getString("languages");
                String is_oversea = rs.getString("is_oversea");
                String recruit_type = String.valueOf(rs.getInt("recruit_type"));
                String nature = String.valueOf(rs.getInt("nature"));
                String is_inside = String.valueOf(rs.getInt("is_inside"));
                String is_secret = String.valueOf(rs.getInt("is_secret"));
                String number = String.valueOf(rs.getInt("number"));
                String gender = String.valueOf(rs.getInt("gender"));
                String profession = rs.getString("profession");
                String is_pa_researched = String.valueOf(rs.getInt("is_pa_researched"));
                String recommand_resume_count = String.valueOf(rs.getInt("recommand_resume_count"));
                String referral_reward = rs.getString("referral_reward");
                String occupation_commercial_activitie = rs.getString("occupation_commercial_activitie");
                String company_commercial_activitie = rs.getString("company_commercial_activitie");
                String is_manager = rs.getString("is_manager");
                String subordinate = String.valueOf(rs.getInt("subordinate"));
                String manager_years = String.valueOf(rs.getInt("manager_years"));
                String tags = rs.getString("tags");
                String description = rs.getString("description");
                String requirement = rs.getString("requirement");
                String department_desc = rs.getString("department_desc");
                String additional_desc = rs.getString("additional_desc");
                String processing_rate = rs.getString("processing_rate");
                String show_count = rs.getString("show_count");
                String last_source = rs.getString("last_source");
                String last_source_id = rs.getString("last_source_id");
                String refreshed_info = rs.getString("refreshed_info");
                String user_ids = rs.getString("user_ids");
                String organization = rs.getString("organization");
                String hp_research = rs.getString("hp_research");
                String special_period = rs.getString("special_period");
                String pa_info = rs.getString("pa_info");
                String custom_data = rs.getString("custom_data");
                String is_urgent = rs.getString("is_urgent");
                String pa_urgent_level = rs.getString("pa_urgent_level");
                String is_headhunter = rs.getString("is_headhunter");
                String is_headhunter_trade = rs.getString("is_headhunter_trade");
                String is_interpolate = rs.getString("is_interpolate");
                String is_hot = rs.getString("is_hot");
                String is_deleted = rs.getString("is_deleted");
                String createdAt = "";
                Timestamp create = null != rs.getObject("created_at") ? rs.getTimestamp("created_at") : null;
                if (null != create) {
                    LocalDateTime localDateTime = create.toLocalDateTime();
                    createdAt = localDateTime.format(dtf);
                }
                String updatedAt = "";
                Timestamp update = null != rs.getObject("updated_at") ? rs.getTimestamp("updated_at") : null;
                if (null != update) {
                    LocalDateTime localDateTime = update.toLocalDateTime();
                    updatedAt = localDateTime.format(dtf);
                }
                String pa_refreshed_at = "";
                Timestamp pa_refreshed = null != rs.getObject("pa_refreshed_at") ? rs.getTimestamp("pa_refreshed_at") : null;
                if (null != pa_refreshed) {
                    LocalDateTime localDateTime = pa_refreshed.toLocalDateTime();
                    pa_refreshed_at = localDateTime.format(dtf);
                }
                result.put("id", id);
                result.put("email", email);
                result.put("strategy_type", strategy_type);
                result.put("position_type", position_type);
                result.put("shop_id", shop_id);
                result.put("relation_id", relation_id);
                result.put("category", category);
                result.put("hunter_industry", hunter_industry);
                result.put("hunter_suspended", hunter_suspended);
                result.put("hunter_protection_period", hunter_protection_period);
                result.put("hunter_pay_end", hunter_pay_end);
                result.put("hunter_pay_begin", hunter_pay_begin);
                result.put("hunter_salary_end", hunter_salary_end);
                result.put("hunter_salary_begin", hunter_salary_begin);
                result.put("category_id", category_id);
                result.put("real_corporation_name", real_corporation_name);
                result.put("address", address);
                result.put("salary", salary);
                result.put("source_ids", source_ids);
                result.put("project_ids", project_ids);
                result.put("languages", languages);
                result.put("is_oversea", is_oversea);
                result.put("recruit_type", recruit_type);
                result.put("nature", nature);
                result.put("is_inside", is_inside);
                result.put("is_secret", is_secret);
                result.put("number", number);
                result.put("gender", gender);
                result.put("profession", profession);
                result.put("is_pa_researched", is_pa_researched);
                result.put("recommand_resume_count", recommand_resume_count);
                result.put("referral_reward", referral_reward);
                result.put("occupation_commercial_activitie", occupation_commercial_activitie);
                result.put("company_commercial_activitie", company_commercial_activitie);
                result.put("is_manager", is_manager);
                result.put("subordinate", subordinate);
                result.put("manager_years", manager_years);
                result.put("tags", tags);
                result.put("description", description);
                result.put("requirement", requirement);
                result.put("department_desc", department_desc);
                result.put("additional_desc", additional_desc);
                result.put("processing_rate", processing_rate);
                result.put("show_count", show_count);
                result.put("last_source", last_source);
                result.put("last_source_id", last_source_id);
                result.put("refreshed_info", refreshed_info);
                result.put("user_ids", user_ids);
                result.put("organization", organization);
                result.put("hp_research", hp_research);
                result.put("special_period", special_period);
                result.put("pa_info", pa_info);
                result.put("custom_data", custom_data);
                result.put("is_urgent", is_urgent);
                result.put("pa_urgent_level", pa_urgent_level);
                result.put("is_headhunter", is_headhunter);
                result.put("is_headhunter_trade", is_headhunter_trade);
                result.put("is_interpolate", is_interpolate);
                result.put("is_hot", is_hot);
                result.put("is_deleted", is_deleted);
                result.put("created_at", createdAt);
                result.put("updated_at", updatedAt);
                result.put("pa_refreshed_at", pa_refreshed_at);
                list.add(result);
            }
            free();
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return list;
    }


    public String getAlgorithmsCvTag(String sql) throws SQLException {
        String cvTag = "";
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                byte[] cvTagsByte = rs.getBytes("cv_tag");
                cvTag = null != cvTagsByte ? new String(cvTagsByte, StandardCharsets.UTF_8) : "";
            }
            free();
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return cvTag;
    }

    public Map<String, String> getAlgorithmsCvTagAndCvTrade(String sql) throws SQLException {
        Map<String, String> result = new HashMap<>();
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                byte[] cvTagsByte = rs.getBytes("cv_tag");
                byte[] cvTradeByte = rs.getBytes("cv_trade");
                String cvTag = null != cvTagsByte ? new String(cvTagsByte, StandardCharsets.UTF_8) : "";
                String cvTrade = null != cvTradeByte ? new String(cvTradeByte, StandardCharsets.UTF_8) : "";
                result.put("cv_tag", cvTag);
                result.put("cv_trade", cvTrade);
            }
            free();
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return result;
    }

    public String getAlgorithmsCvTagForTob(String sql) throws SQLException {
        String cvTag = "";
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                byte[] cvTagsByte = rs.getBytes("cv_tag");
                String cvTagStr = null != cvTagsByte ? new String(cvTagsByte, StandardCharsets.UTF_8) : "";
                //解压
                if (StringUtils.isNoneBlank(cvTagStr)) {
                    cvTag = new String(MyString.gzipUncompress(Base64.getDecoder().decode(cvTagStr.trim())));
                }
            }
            free();
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return cvTag;
    }

    public Map<String, String> getAlgorithmsCvTagAndTradeForTob(String sql) throws SQLException {
        Map<String, String> result = new HashMap<>();
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                byte[] cvTagsByte = rs.getBytes("cv_tag");
                byte[] cvTradeByte = rs.getBytes("cv_trade");
                String cvTagStr = null != cvTagsByte ? new String(cvTagsByte, StandardCharsets.UTF_8) : "";
                String cvTradeStr = null != cvTradeByte ? new String(cvTradeByte, StandardCharsets.UTF_8) : "";
                //解压
                String cvTag = "";
                if (StringUtils.isNoneBlank(cvTagStr)) {
                    cvTag = new String(MyString.gzipUncompress(Base64.getDecoder().decode(cvTagStr.trim())));

                }
                String cvTrade="";
                if (StringUtils.isNoneBlank(cvTradeStr)) {
                    cvTrade = new String(MyString.gzipUncompress(Base64.getDecoder().decode(cvTradeStr.trim())));

                }
                result.put("cv_tag", cvTag);
                result.put("cv_trade", cvTrade);
            }
            free();
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return result;
    }

    public String queryCompress(String sql) throws Exception {
        String compressStr = "";
        pstmt = conn.prepareStatement(sql);
        rs = pstmt.executeQuery();
        while (rs.next()) {
            try {
                byte[] compressByte = rs.getBytes("compress");
                if (null != compressByte) {
                    String compress = new String(compressByte, StandardCharsets.UTF_8);
                    if (StringUtils.isNotBlank(compress)) {
                        byte[] bytes = MyString.hexStringToBytes(compress);
                        compressStr = MyString.unzipString(bytes);
                    }
                }

            } catch (Exception e) {
                try {
                    Blob compress = rs.getBlob("compress");
                    if (null != compress) {
                        compressStr = MyString
                            .unzipString(compress.getBytes(1, (int) compress.length()));
                    }
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }
        }
        free();
        return compressStr;
    }


    public String queryCompressAndAlgorithms(String sql) throws Exception {
        String result = "";
        pstmt = conn.prepareStatement(sql);
        rs = pstmt.executeQuery();
        while (rs.next()) {
            try {
//                String compress = rs.getString("compress");
//                String algorithmsData = rs.getString("algorithms_data");
                String compress = new String(rs.getBytes("compress"), StandardCharsets.UTF_8);
                String algorithmsData = new String(rs.getBytes("algorithms_data"), StandardCharsets.UTF_8);
                if (StringUtils.isNotBlank(compress)) {
                    byte[] bytes = MyString.hexStringToBytes(compress);
                    String compressStr = MyString.unzipString(bytes);
                    JSONObject json = JSONObject.parseObject(compressStr);
                    if (StringUtils.isNoneBlank(algorithmsData)) {
                        JSONObject jsonObject = JSONObject.parseObject(algorithmsData);
                        json.put("algorithm", jsonObject);
                    } else {
                        json.put("algorithm", new JSONObject());
                    }
                    result = JSON.toJSONString(json);
                }
            } catch (Exception e) {
                try {
                    Blob compress = rs.getBlob("compress");
//                String algorithmsData = rs.getString("algorithms_data");
                    String algorithmsData = new String(rs.getBytes("algorithms_data"), StandardCharsets.UTF_8);
                    if (null != compress) {
                        String compressStr = MyString
                            .unzipString(compress.getBytes(1, (int) compress.length()));
                        if (StringUtils.isNoneBlank(compressStr)) {
                            JSONObject json = JSONObject.parseObject(compressStr);
                            if (StringUtils.isNoneBlank(algorithmsData)) {
                                JSONObject jsonObject = JSONObject.parseObject(algorithmsData);
                                json.put("algorithm", jsonObject);
                            } else {
                                json.put("algorithm", new JSONObject());
                            }
                            result = JSON.toJSONString(json);
                        }
                    }
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }
        }
        free();
        return result;
    }

    //select a.resume_content,column_json(b.data) as data from `tob_resume_pool_0`.resume_detail_0 a left join `tob_algorithms_pool_0`.algorithms_0 b on a.tob_resume_id=b.tob_resume_id where a.tob_resume_id=1293682772659404800
    public Map<String, String> getTobCvJSON(String sql) throws Exception {
        Map<String, String> result = new HashMap<>();
        pstmt = conn.prepareStatement(sql);
        rs = pstmt.executeQuery();
        while (rs.next()) {
            try {
                String resumeContent = new String(rs.getBytes("resume_content"), StandardCharsets.UTF_8);
                String algorithmsData = new String(rs.getBytes("data"), StandardCharsets.UTF_8);
                result.put("resume_content", resumeContent);
                result.put("data", algorithmsData);
            } catch (Exception e) {
                logger.error("process sql:{} error:{}", sql, e.getMessage());
            }
        }
        free();
        return result;
    }


    public int executeCount(String sql) throws SQLException {
        busy();
        int result = 0;
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql);
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    result = null != rs.getObject("number") ? rs.getInt("number") : 0;
                }
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
        return result;
    }


    /**
     * insert 返回主键id
     */
    public int executeInsert(String sql) throws SQLException {
        busy();
        int id = 0;
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                pstmt.executeUpdate();
                ResultSet res = pstmt.getGeneratedKeys();
                while (res.next()) {
                    id = res.getInt(1);
                }
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
        return id;
    }

    public int executeUpdate(String sql) throws SQLException {
        busy();
        int result = 0;
        try {
            pstmt = conn.prepareStatement(sql);
            result = pstmt.executeUpdate();
            free();
        } catch (SQLException ex) {
            processException(ex);
        }
        return result;
    }

    /**
     * insert 返回主键id
     */
    public int executeInsertByJson(String sql, JSONObject jsons, BigDecimal lng, BigDecimal lat,
                                   long addressId) throws SQLException {
        busy();
        int id = 0;
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                String stationName = jsons.getString("name");
                JSONObject location = jsons.getJSONObject("location");
                BigDecimal sLat = location.getBigDecimal("lat");
                BigDecimal sLng = location.getBigDecimal("lng");
                String sAddress = jsons.getString("address");
                int transportation = 0;
                JSONObject detailInfo = jsons.getJSONObject("detail_info");
                int distance = detailInfo.getInteger("distance");
                pstmt.setBigDecimal(1, lng);
                pstmt.setBigDecimal(2, lat);
                pstmt.setString(3, stationName);
                pstmt.setBigDecimal(4, sLng);
                pstmt.setBigDecimal(5, sLat);
                pstmt.setLong(6, addressId);
                pstmt.setInt(7, transportation);
                pstmt.setInt(8, distance);
                pstmt.setString(9, sAddress);
                pstmt.executeUpdate();
                ResultSet res = pstmt.getGeneratedKeys();
                while (res.next()) {
                    id = res.getInt(1);
                }
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
        return id;
    }

    /**
     * 批量插入数据
     */
    public void executeBatchInsertByArray(String sql, String[] strs, int tid) throws SQLException {
        busy();
        while (true) {
            conn.setAutoCommit(false);
            try {
                pstmt = conn.prepareStatement(sql);
                for (String str : strs) {
                    pstmt.setInt(1, tid);
                    pstmt.setString(2, str);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
                conn.commit();
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
    }


    public boolean execute(String sql) throws SQLException {
        busy();
        boolean result = true;
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql);
                result = pstmt.execute(sql);
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
                result = false;
            }
        }
        return result;
    }

    public boolean saveResumeAlgorithms(String sql) throws SQLException {
        busy();
        boolean result = true;
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql);
                pstmt.executeUpdate();
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
                result = false;
            }
        }
        return result;
    }


    public boolean saveTobAlgorithms(String sql) throws SQLException {
        busy();
        boolean result = true;
        try {
            pstmt = conn.prepareStatement(sql);
            pstmt.executeUpdate();
            free();
        } catch (SQLException ex) {
            result = false;
            processException(ex);
        }
        return result;
    }

    //| id         | bigint(20) unsigned | NO   | PRI | 0                 |                             |
    //| name       | varchar(62)         | NO   |     | NULL              |                             |
    //| tel        | varchar(256)        | NO   |     | NULL              |                             |
    //| phone      | varchar(72)         | NO   |     | NULL              |                             |
    //| email      | varchar(512)        | YES  |     | NULL              |                             |
    //| qq         | varchar(72)         | NO   |     | NULL              |                             |
    //| msn        | varchar(72)         | NO   |     | NULL              |                             |
    //| sina       | varchar(72)         | NO   |     | NULL              |                             |
    //| ten        | varchar(72)         | NO   |     | NULL              |                             |
    //| wechat     | varchar(32)         | NO   |     |                   |                             |
    //| phone_area | tinyint(2)          | NO   |     | NULL              |                             |
    //| is_deleted | enum('Y','N')       | NO   |     | N                 |                             |
    //| updated_at | timestamp           | NO   | MUL | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
    //| created_at | timestamp           | NO   |     | CURRENT_TIMESTAMP

    //insert into contacts(`id`,`name`,`tel`,`phone`,`email`,`qq`,`msn`,`sina`,`ten`,`wechat`,`phone_area`,`is_deleted`,`updated_at`,`created_at`)
    // values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    public int saveContacts(String sql, Map<String, Object> contactMap) throws SQLException {
        busy();
        int flag = 0;
        try {
            pstmt = conn.prepareStatement(sql);
            long id = Long.parseLong(String.valueOf(contactMap.get("id")));
            pstmt.setLong(1, id);
            pstmt.setString(2, String.valueOf(contactMap.get("name")));
            pstmt.setString(3, String.valueOf(contactMap.get("tel")));
            pstmt.setString(4, String.valueOf(contactMap.get("phone")));
            pstmt.setString(5, String.valueOf(contactMap.get("email")));
            pstmt.setString(6, String.valueOf(contactMap.get("qq")));
            pstmt.setString(7, String.valueOf(contactMap.get("msn")));
            pstmt.setString(8, String.valueOf(contactMap.get("sina")));
            pstmt.setString(9, String.valueOf(contactMap.get("ten")));
            pstmt.setString(10, String.valueOf(contactMap.get("wechat")));
            pstmt.setInt(11, (int) contactMap.get("phone_area"));
            pstmt.setString(12, String.valueOf(contactMap.get("is_deleted")));
            String updatedAt = String.valueOf(contactMap.get("updated_at"));
            Timestamp updated = Timestamp.valueOf(updatedAt);
            pstmt.setTimestamp(13, updated);
            String createdAt = String.valueOf(contactMap.get("created_at"));
            Timestamp created = Timestamp.valueOf(createdAt);
            pstmt.setTimestamp(14, created);
            flag = pstmt.executeUpdate();
            free();
        } catch (SQLException ex) {
            processException(ex);
        }
        return flag;
    }


    public List<String> listTable() throws SQLException {
        busy();
        List<String> tables = new ArrayList<String>();
        while (true) {
            try {
                pstmt = conn.prepareStatement(String.format("SHOW TABLES FROM `%s`", tableName));
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    tables.add(rs.getObject("Tables_in_".concat(tableName)).toString());
                }
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
        return tables;
    }

//    public List<String> listDatabase() throws SQLException {
//        busy();
//        List<String> databases = new ArrayList<String>();
//        while (true) {
//            try {
//                pstmt = conn.prepareStatement("show databases");
//                rs = pstmt.executeQuery();
//                while (rs.next()) {
//                    databases.add(rs.getObject("Database").toString());
//                }
//                free();
//                break;
//            } catch (SQLException ex) {
//                processException(ex);
//            }
//        }
//        return databases;
//    }

    public void busy() {
        isBusy = true;
    }

    public void free() throws SQLException {
        isBusy = false;
        if (pstmt != null) {
            pstmt.close();
            pstmt = null;
        }
        if (rs != null) {
            rs.close();
            rs = null;
        }
    }

    public boolean isbusy() {
        return isBusy;
    }

    public void setTableName(String table) throws SQLException {
        tableName = table;
        execute("use `" + table + "`");
    }

    public boolean isValid() {
        try {
            return conn.isValid(3000);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean isSupportsBatchUpdates() throws SQLException {
        //conn是Connection的类型
        DatabaseMetaData dbmd = conn.getMetaData();
        //为true则意味着该数据是支持批量更新的
        return dbmd.supportsBatchUpdates();
    }

    public Connection getConn() {
        reConnect();
        return conn;
    }

    private void reConnect() {
        while (true) {
            try {
                free();
                if (!conn.isValid(3000)) {
                    close();
                    try {
                        createConn();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                break;
            } catch (SQLException ex) {
                ex.printStackTrace();
                try {
                    Thread.currentThread().sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private boolean processException(SQLException ex) throws SQLException {
        int err_code = ex.getErrorCode();
        String err_msg = ex.getMessage();
        System.out.println("_._._._._._._._._._" + err_code + ":" + err_msg);
        if (!(err_code != 2013 && err_code != 2006 && err_code != 1053 && !err_msg
            .contains("No operations allowed after connection closed") && !err_msg
            .contains("The last packet successfully received from"))) {
            ex.printStackTrace();
            reConnect();
        } else {
            throw new SQLException(ex.getMessage(), ex.getSQLState(), err_code);
        }
        return true;
    }


}
