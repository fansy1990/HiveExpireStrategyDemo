package quartz;

import com.alibaba.fastjson.JSON;
import hive.HiveUtil;
import model.RedisHiveTableData;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.JedisUtil;

import java.util.Set;

/**
 * 定时扫描Redis，并检测是否里面的Hive表expired
 * Created by fansy on 2017/7/7.
 */
public class ScanJob  implements Job{
    private Logger logger = LoggerFactory.getLogger(ScanJob.class);
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
       Set<String> keys = JedisUtil.getKeys(HiveUtil.tableKeyPrefix + "*");
        RedisHiveTableData redisHiveTableData;
        if(!keys.isEmpty()){
            for(String key:keys){
                redisHiveTableData = JSON.parseObject(JedisUtil.getValue(key), RedisHiveTableData.class);
                if(redisHiveTableData.expired()){
                    HiveUtil.deleteTable(redisHiveTableData.getTableName());
                }else{
                    logger.info("Hive 表{} 未过期。", redisHiveTableData.getTableName());
                }
            }
        }else{
            logger.info("Redis中没有存储任何Hive表!");
        }
    }
}
