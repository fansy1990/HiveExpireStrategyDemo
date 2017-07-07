package quartz;

import common.PropertiesUtil;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

/**
 * 定时任务设置
 * Created by fansy on 2017/7/7.
 */
public class CronTrigger implements  Runnable{

    private static final String CRONFILE="cron.properties";
    private static final String CRON_EXPRESSION="cron.expression";


    @Override
    public void run() {

        JobDetail job = JobBuilder.newJob(ScanJob.class)
                .withIdentity("dummyJobName", "group1").build();

        Trigger trigger = TriggerBuilder
                .newTrigger()
                .withIdentity("dummyTriggerName", "group1")
                .withSchedule(
                        CronScheduleBuilder.cronSchedule(PropertiesUtil.getProperty(CRONFILE,CRON_EXPRESSION)))
                .build();

        //schedule it
        Scheduler scheduler = null;
        try {
            scheduler = new StdSchedulerFactory().getScheduler();
            scheduler.start();
            scheduler.scheduleJob(job, trigger);
        } catch (SchedulerException e) {
            e.printStackTrace();
        }

    }
}
