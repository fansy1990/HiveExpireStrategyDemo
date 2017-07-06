package demo;

import com.alibaba.fastjson.JSON;
import hive.HiveUtil;
import model.HiveTableData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.JedisUtil;

import java.util.Set;

/**
 * Hive 表设置expire定时删除 ，借助于Redis
 * Created by fansy on 2017/7/6.
 */
public class HiveCreateExpireTable {
    private static Logger logger = LoggerFactory.getLogger(HiveCreateExpireTable.class);

    public static void main(String[] args) throws InterruptedException {

        // 1. 创建Hive表

        for(int i=1 ; i < 4 ; i++){
            Thread.sleep(1000);
            HiveUtil.createTable("t"+i,i);
        }

        boolean flag = true;
        Set<String> keys = null;
        HiveTableData hiveTableData =null;
        int count =0;
        while(flag){
            count++;
            logger.info("第{}次轮训...",count);
            Thread.sleep(5000);
            keys = JedisUtil.getKeys(HiveUtil.tableKeyPrefix+"*");
            if(!keys.isEmpty()){
                for(String key:keys){
                    hiveTableData = JSON.parseObject(JedisUtil.getValue(key),HiveTableData.class);
                    if(hiveTableData.expired()){
                        HiveUtil.deleteTable(hiveTableData.getTableName());
                    }else{
                        logger.info("Hive 表{} 未过期。",hiveTableData.getTableName());
                    }
                }
            }else{
                flag = false;
            }
        }
        logger.info("All done!");
    }
}
