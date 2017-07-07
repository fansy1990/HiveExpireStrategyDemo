package model;

import java.util.Date;

/**
 * Created by fansy on 2017/7/6.
 */
public class RedisHiveTableData {
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getLiveTime() {
        return liveTime;
    }

    public void setLiveTime(int liveTime) {
        this.liveTime = liveTime;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    private String tableName;
    private Date startDate;
    private int liveTime; // minutes

    public RedisHiveTableData(){}
    public RedisHiveTableData(String tableName, int expireTime){
        this.tableName=tableName;
        this.startDate = new Date();
        this.liveTime = expireTime;
    }
    /**
     * 是否过期
     *
     * @return
     */
    public boolean expired() {
        return (System.currentTimeMillis() - startDate.getTime()) / 60 / 1000 >= liveTime;
    }

}
