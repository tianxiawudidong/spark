package com.ifchange.spark.mysql;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * 数据库连接层MYSQL
 *
 * @author Administrator
 */
public class DBConnection {

    public static Connection getDBConnection(String username, String password, String dbName, String host, int port, String encoding) throws SQLException, ClassNotFoundException {
        try {
            // 1. 注册驱动
            Class.forName("com.mysql.jdbc.Driver");
            // 获取数据库的连接
            String conn_url = "jdbc:mysql://" + host + ":" + port + "/" + dbName + "?useUnicode=true&characterEncoding=" + encoding + "&useSSL=false&zeroDateTimeBehavior=convertToNull";
            return java.sql.DriverManager.getConnection(conn_url, username, password);
        } catch (ClassNotFoundException e) {
            throw new ClassNotFoundException(e.getMessage());
        } catch (SQLException e1) {
            throw new SQLException(e1);
        }
    }

    public static Connection getDBConnection(String username, String password, String dbName, String host, int port) throws SQLException, ClassNotFoundException {
        return DBConnection.getDBConnection(username, password, dbName, host, port, "utf-8");
    }

    public static Connection getDBConnection(String username, String password, String dbName, String host) throws SQLException, ClassNotFoundException {
        return DBConnection.getDBConnection(username, password, dbName, host, 3306, "utf-8");
    }

    public static Connection getDBConnection(String username, String password, String dbName) throws SQLException, ClassNotFoundException {
        return DBConnection.getDBConnection(username, password, dbName, "127.0.0.1", 3306, "utf-8");
    }

    public static Connection getDBConnection(String username, String password) throws SQLException, ClassNotFoundException {
        return DBConnection.getDBConnection(username, password, "mysql", "127.0.0.1", 3306, "utf-8");
    }
}
