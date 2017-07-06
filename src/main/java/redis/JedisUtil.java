package redis;

import com.alibaba.fastjson.JSON;
import hive.HiveUtil;
import model.HiveTableData;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import java.util.Map;
import java.util.Set;

/**
 * Created by fansy on 2017/7/6.
 */
public class JedisUtil {
    private static final String host="localhost";
    private static final int port =6379 ;
    private static final String password="redis";
    private static final int db =1;
    private static JedisPool jedisPool;
    private static Jedis jedis;
    /**
     * 获取客户端连接
     * @return
     */
    private static Jedis getJedis(){
        if(jedis == null){
            JedisPoolConfig config = new JedisPoolConfig();
            jedisPool = new JedisPool(config,host,port, Protocol.DEFAULT_TIMEOUT,password,db);
            jedis = jedisPool.getResource();
        }
        return jedis;
    }

    public static void put(HiveTableData data){
        getJedis().set(HiveUtil.tableKeyPrefix+data.getTableName(),
                JSON.toJSONString(data));

    }

    public static void del(String tableName){
        getJedis().del(HiveUtil.tableKeyPrefix+tableName);
    }

    public static Set<String> getKeys(String pattern){
        return getJedis().keys(pattern);
    }

    public static String getValue(String key){
        return getJedis().get(key);
    }

    public static void close(){
        jedis.close();
        jedisPool.close();
    }
}
