package hive;

import common.PropertiesUtil;
import model.RedisHiveTableData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.JedisUtil;

import java.sql.*;
import java.util.Properties;


/**
 * Created by fansy on 2017/7/6.
 */
public class HiveUtil {
    public static final String tableKeyPrefix ="table_";// redis存储中的key的前缀
    private static Logger logger = LoggerFactory.getLogger(HiveUtil.class);

    private static final String HIVEFILE="hive.properties";


    private static  String url =null;

    private static  String user = null;

    private static  String password = null;

    private static  String driver = null;

    private static Connection conn = null;

    static{
        Properties properties= PropertiesUtil.getProperty(HIVEFILE);
        url = properties.getProperty("hive.url");
        user = properties.getProperty("hive.user");
        password = properties.getProperty("hive.password");
        driver = properties.getProperty("hive.driver");
    }

    public static void createTable(String sql,String tableName,int expireTime){
        // 1. 添加数据到Redis中
        JedisUtil.put(new RedisHiveTableData(tableName,expireTime));
        // 创建Hive表
        if(executeSqlWithoutResultSet(sql)){
            logger.info("创建Hive表{},失效时间是{}...",new Object[]{tableName,expireTime});
        }else{
            logger.warn("创建Hive表{}失败！", tableName);
        }


    }

    public static void deleteTable(String tableName){
        // 1. 删除Redis中对应key数据
        JedisUtil.del(tableName);
        // 2. 删除Hive表
        if(executeSqlWithoutResultSet("drop table "+ tableName)){
            logger.info("删除Hive表{}成功",tableName);
        }else{
            logger.warn("删除Hive表{}失败！",tableName);
        }

    }


    private static Connection getConn(){
        if(conn != null){
            return conn;
        }
        try {
            Class.forName(driver);
            return DriverManager.getConnection(url, user, password);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }

    /**
     * 执行没有返回值的 sql
     * @param sql
     * @return
     */
    public static boolean executeSqlWithoutResultSet(String sql){
        Statement stmt=null;
        try {
            stmt = getConn().createStatement();
            stmt.execute(sql);
            stmt.close();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 执行有返回值的 sql
     * TODO 待完善， 如果rs关闭或者stmt关闭，那么rs就获取不了值了。
     * @param sql
     * @return
     */
    public static ResultSet executeSqlWithResultSet(String sql){
        Statement stmt=null;
        ResultSet rs = null;
        try {
            stmt = getConn().createStatement();
            rs = stmt.executeQuery(sql);
            return rs;
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                rs.close();
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
