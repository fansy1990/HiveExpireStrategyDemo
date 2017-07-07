package demo;

import hive.HiveUtil;

/**
 * Created by fansy on 2017/7/7.
 */
public class HiveUtilTest {
    public static void main(String[] args) {
        String sql = "create table demo01 (id int , age int, name string)";

        System.out.println(HiveUtil.executeSqlWithoutResultSet(sql));
    }
}
