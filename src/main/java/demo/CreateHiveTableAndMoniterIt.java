package demo;

import hive.HiveUtil;
import quartz.CronTrigger;
import redis.JedisUtil;

/**
 * Created by fansy on 2017/7/7.
 */
public class CreateHiveTableAndMoniterIt {

    public static void main(String[] args) throws InterruptedException {
        // 1. 线程启动ScanJob，每cron.expression秒刷新一次
        new Thread(new CronTrigger()).start();

        // 2. 等待30秒
        Thread.sleep(30000);

        // 3. 创建表 demo01, 指定live time 为1分钟
        HiveUtil.createTable("create table demo01(id int,age int)","demo01",1);

        // 4. 创建表 demo02, 指定live time 为2分钟
        HiveUtil.createTable("create table demo02(id int,age int)","demo02",2);

        // 5. 创建表 demo03, 指定live time 为3分钟
        HiveUtil.createTable("create table demo03(id int,age int)","demo03",3);

        boolean flag = true;
        while(flag){// 主线程等待
            Thread.sleep(3000);
            if(JedisUtil.getKeys(HiveUtil.tableKeyPrefix+"*").size() <1){
                flag = false;
            }
        }

        System.out.println("All done!");
        Thread.sleep(10000);
        System.exit(0);
    }
}
