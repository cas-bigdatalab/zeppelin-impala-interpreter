/**
 * Project Name:zeppelin-impala
 * File Name:ImpalaTest
 * Package Name:org.apache.zeppelin.impala
 * Author : moonx
 * Date:2016/9/29
 * Copyright (c) 2016,  All Rights Reserved.
 * <p>
 * <p>
 * _ooOoo_
 * o8888888o
 * 88" . "88
 * (| -_- |)
 * O\ = /O
 * ____/`---'\____
 * .   ' \\| |// `.
 * / \\||| : |||// \
 * / _||||| -:- |||||- \
 * | | \\\ - /// | |
 * | \_| ''\---/'' | |
 * \ .-\__ `-` ___/-. /
 * ___`. .' /--.--\ `. . __
 * ."" '< `.___\_<|>_/___.' >'"".
 * | | : `- \`.;`\ _ /`;.`/ - ` : | |
 * \ \ `-. \_ __\ /__ _/ .-` / /
 * ======`-.____`-.___\_____/___.-`____.-'======
 * `=---='
 * ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
 * 佛祖保佑       永无BUG
 * 佛曰:
 * 写字楼里写字间，写字间里程序员；
 * 程序人员写程序，又拿程序换酒钱。
 * 酒醒只在网上坐，酒醉还来网下眠；
 * 酒醉酒醒日复日，网上网下年复年。
 * 但愿老死电脑间，不愿鞠躬老板前；
 * 奔驰宝马贵者趣，公交自行程序员。
 * 别人笑我忒疯癫，我笑自己命太贱；
 * 不见满街漂亮妹，哪个归得程序员？
 */
package org.apache.zeppelin.impala;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

/**
 * Created by moonx on 2016/9/29.
 */
public class ImpalaTest {

    private static String driverName =
            "org.apache.hive.jdbc.HiveDriver";


    @Test
    public void testHive() throws SQLException,ClassNotFoundException {
        Class.forName(driverName);
        Connection con = DriverManager.getConnection(
                "jdbc:hive2://10.0.71.31:10000", "hive", "");
        Statement stmt = con.createStatement();
        String sql = "select * from test" ;
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getString(1)) + "\t"
                    + res.getInt(2));
        }
    }

    @Test
    public void testImpala() throws SQLException,ClassNotFoundException{
        Class.forName(driverName);
        Connection con = DriverManager.getConnection(
                "jdbc:hive2://10.0.71.19:21050/;auth=noSasl", "hive", "");
        Statement stmt = con.createStatement();
        String sql = "select * from test_1" ;
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getString(1)) + "\t"
                    + res.getInt(2));
        }
    }
}
