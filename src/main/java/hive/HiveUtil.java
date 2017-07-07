package hive;

import model.RedisHiveTableData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.JedisUtil;


/**
 * Created by fansy on 2017/7/6.
 */
public class HiveUtil {
    public static final String tableKeyPrefix ="table_";// redis存储中的key的前缀
    private static Logger logger = LoggerFactory.getLogger(HiveUtil.class);

    public static void createTable(String tableName,int expireTime){
        // 1. 添加数据到Redis中
        JedisUtil.put(new RedisHiveTableData(tableName,expireTime));
        // 创建Hive表
        logger.info("创建Hive表{},失效时间是{}...",new Object[]{tableName,expireTime});
    }

    public static void deleteTable(String tableName){
        // 1. 删除Redis中对应key数据
        JedisUtil.del(tableName);
        // 2. 删除Hive表
        logger.info("删除Hive表{}...",tableName);
    }
}
