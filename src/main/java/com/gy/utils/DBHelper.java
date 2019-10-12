package com.gy.utils;

import com.alibaba.druid.pool.DruidDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * created by yangyu on 2019-09-17
 */
@Configuration
@ConfigurationProperties(prefix = "datasource")
@PropertySource("mysql.properties")
public class DBHelper {

    private static final Logger logger = LoggerFactory.getLogger(DBHelper.class);

    private String icUrl;

    private String tcUrl;

    private String reportUrl;

    private String className;

    private String username;
    private String password;

    private static DruidDataSource icDataSource = null;
    private static DruidDataSource tcDataSource = null;
    private static DruidDataSource reportDataSource = null;

    private static ThreadLocal<DruidDataSource> icThredLocal = ThreadLocal.withInitial(DruidDataSource::new);
    private static ThreadLocal<DruidDataSource> tcThredLocal = ThreadLocal.withInitial(DruidDataSource::new);
    private static ThreadLocal<DruidDataSource> reportThredLocal = ThreadLocal.withInitial(DruidDataSource::new);

    @Bean(name = "icConn")
    @Primary
    public DataSource icConn() {
        icDataSource = icThredLocal.get();
        try {
            icDataSource.setUrl(icUrl);
            icDataSource.setDriverClassName(className);
            icDataSource.setUsername(username);
            icDataSource.setPassword(password);

        } catch (Exception ex) {
            logger.error("Get IC JDBC Connection failed: ",ex);
        }
        return icDataSource;
    }

    @Bean(name = "tcConn")
    public DataSource tcConn() {
        tcDataSource = tcThredLocal.get();
        try {
            tcDataSource.setUrl(tcUrl);
            tcDataSource.setDriverClassName(className);
            tcDataSource.setUsername(username);
            tcDataSource.setPassword(password);

        } catch (Exception ex) {
            logger.error("Get TC JDBC Connection failed: ",ex);
        }
        return tcDataSource;
    }

    @Bean(name = "reportConn")
    public DataSource reportConn() {
        reportDataSource = reportThredLocal.get();
        try {
            reportDataSource.setUrl(reportUrl);
            reportDataSource.setDriverClassName(className);
            reportDataSource.setUsername(username);
            reportDataSource.setPassword(password);

        } catch (Exception ex) {
            logger.error("Get Report JDBC Connection failed: ",ex);
        }
        return reportDataSource;
    }

    public static Connection getConnection(String dbName) throws SQLException {
        switch (dbName){
            case "ic":
                return icDataSource.getConnection();
            case "tc":
                return tcDataSource.getConnection();
            case "report":
                return reportDataSource.getConnection();
            default:
                return null;
        }
    }

    public String getIcUrl() {
        return icUrl;
    }

    public void setIcUrl(String icUrl) {
        this.icUrl = icUrl;
    }

    public String getTcUrl() {
        return tcUrl;
    }

    public void setTcUrl(String tcUrl) {
        this.tcUrl = tcUrl;
    }

    public String getReportUrl() {
        return reportUrl;
    }

    public void setReportUrl(String reportUrl) {
        this.reportUrl = reportUrl;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
