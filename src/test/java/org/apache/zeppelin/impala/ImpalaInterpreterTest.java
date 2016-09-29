/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.zeppelin.impala;

import static java.lang.String.format;
import static org.apache.zeppelin.interpreter.Interpreter.logger;
import static org.junit.Assert.assertEquals;
import static org.apache.zeppelin.impala.ImpalaInterpreter.DEFAULT_KEY;
import static org.apache.zeppelin.impala.ImpalaInterpreter.DEFAULT_DRIVER;
import static org.apache.zeppelin.impala.ImpalaInterpreter.DEFAULT_PASSWORD;
import static org.apache.zeppelin.impala.ImpalaInterpreter.DEFAULT_USER;
import static org.apache.zeppelin.impala.ImpalaInterpreter.DEFAULT_URL;
import static org.apache.zeppelin.impala.ImpalaInterpreter.COMMON_MAX_LINE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.List;
import java.util.Properties;

import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.impala.ImpalaInterpreter;
import org.apache.zeppelin.scheduler.FIFOScheduler;
import org.apache.zeppelin.scheduler.ParallelScheduler;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.junit.Before;
import org.junit.Test;

import com.mockrunner.jdbc.BasicJDBCTestCaseAdapter;
/**
 * JDBC interpreter unit tests
 */
public class ImpalaInterpreterTest extends BasicJDBCTestCaseAdapter {

    static String jdbcConnection = "jdbc:hive2://10.0.71.19:21050/";
    InterpreterContext interpreterContext;

    private static String getJdbcConnection() throws IOException {
        return jdbcConnection;
    }

    public static Properties getJDBCTestProperties() {
        Properties p = new Properties();
        p.setProperty("default.driver", "org.apache.hive.jdbc.HiveDriver");
        p.setProperty("default.url", jdbcConnection);
        p.setProperty("default.user", "hive");
        p.setProperty("default.password", "");
        p.setProperty("common.max_count", "1000");

        return p;
    }

    @Before
    public void setUp() throws Exception {

        interpreterContext = new InterpreterContext("", "1", "", "", new AuthenticationInfo(), null, null, null, null,
                null, null);
    }

    @Test
    public void init() throws  Exception{
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection connection = DriverManager.getConnection(getJdbcConnection());
        Statement statement = connection.createStatement();
        statement.execute("DROP TABLE IF EXISTS test_table; " );

        statement.execute("CREATE TABLE test_table(id varchar, name varchar);");

        PreparedStatement insertStatement = connection.prepareStatement("insert into test_table(id, name) values ('a', 'a_name'),('b', 'b_name'),('c', ?);");
        insertStatement.setString(1, "c_name");
        insertStatement.execute();
    }


    @Test
    public void testForParsePropertyKey() throws IOException {
        ImpalaInterpreter t = new ImpalaInterpreter(new Properties());

        assertEquals(t.getPropertyKey("(fake) select max(cant) from test_table where id >= 2452640"),
                "fake");

        assertEquals(t.getPropertyKey("() select max(cant) from test_table where id >= 2452640"),
                "");

        assertEquals(t.getPropertyKey(")fake( select max(cant) from test_table where id >= 2452640"),
                "default");

        // when you use a %jdbc(prefix1), prefix1 is the propertyKey as form part of the cmd string
        assertEquals(t.getPropertyKey("(prefix1)\n select max(cant) from test_table where id >= 2452640"),
                "prefix1");

        assertEquals(t.getPropertyKey("(prefix2) select max(cant) from test_table where id >= 2452640"),
                "prefix2");

        // when you use a %jdbc, prefix is the default
        assertEquals(t.getPropertyKey("select max(cant) from test_table where id >= 2452640"),
                "default");
    }

    @Test
    public void testForMapPrefix() throws SQLException, IOException {
        Properties properties = new Properties();
        properties.setProperty("common.max_count", "1000");
        properties.setProperty("common.max_retry", "3");
        properties.setProperty("default.driver", "org.apache.hive.jdbc.HiveDriver");
        properties.setProperty("default.url", getJdbcConnection());
        properties.setProperty("default.user", "");
        properties.setProperty("default.password", "");
        ImpalaInterpreter t = new ImpalaInterpreter(properties);
        t.open();

        String sqlQuery = "(fake) select * from test_table";

        InterpreterResult interpreterResult = t.interpret(sqlQuery, interpreterContext);

        // if prefix not found return ERROR and Prefix not found.
        assertEquals(InterpreterResult.Code.ERROR, interpreterResult.code());
        assertEquals("Prefix not found.", interpreterResult.message());
    }

    @Test
    public void testDefaultProperties() throws SQLException {
        ImpalaInterpreter ImpalaInterpreter = new ImpalaInterpreter(getJDBCTestProperties());

        assertEquals("org.apache.hive.jdbc.HiveDriver", ImpalaInterpreter.getProperty(DEFAULT_DRIVER));
        assertEquals(jdbcConnection, ImpalaInterpreter.getProperty(DEFAULT_URL));
        assertEquals("hive", ImpalaInterpreter.getProperty(DEFAULT_USER));
        assertEquals("", ImpalaInterpreter.getProperty(DEFAULT_PASSWORD));
        assertEquals("1000", ImpalaInterpreter.getProperty(COMMON_MAX_LINE));
    }

    @Test
    public void testSelectQuery() throws SQLException, IOException {
        Properties properties = new Properties();
        properties.setProperty("common.max_count", "1000");
        properties.setProperty("common.max_retry", "3");
        properties.setProperty("default.driver", "org.apache.hive.jdbc.HiveDriver");
        properties.setProperty("default.url", getJdbcConnection());
        properties.setProperty("default.user", "");
        properties.setProperty("default.password", "");
        ImpalaInterpreter t = new ImpalaInterpreter(properties);
        t.open();

        String sqlQuery = "select * from test_table WHERE ID in ('a', 'b')";

        InterpreterResult interpreterResult = t.interpret(sqlQuery, interpreterContext);

        assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());
        assertEquals(InterpreterResult.Type.TABLE, interpreterResult.type());
        assertEquals("id\tname\na\ta_name\nb\tb_name\n", interpreterResult.message());

    }

    @Test
    public void testSelectQueryWithNull() throws SQLException, IOException {
        Properties properties = new Properties();
        properties.setProperty("common.max_count", "1000");
        properties.setProperty("common.max_retry", "3");
        properties.setProperty("default.driver", "org.apache.hive.jdbc.HiveDriver");
        properties.setProperty("default.url", getJdbcConnection());
        properties.setProperty("default.user", "");
        properties.setProperty("default.password", "");
        ImpalaInterpreter t = new ImpalaInterpreter(properties);
        t.open();

        String sqlQuery = "select * from test_table WHERE ID = 'c'";

        InterpreterResult interpreterResult = t.interpret(sqlQuery, interpreterContext);

        assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());
        assertEquals(InterpreterResult.Type.TABLE, interpreterResult.type());
        assertEquals("id\tname\nc\tc_name\n", interpreterResult.message());
    }


    @Test
    public void testSelectQueryMaxResult() throws SQLException, IOException {

        Properties properties = new Properties();
        properties.setProperty("common.max_count", "1");
        properties.setProperty("common.max_retry", "3");
        properties.setProperty("default.driver", "org.apache.hive.jdbc.HiveDriver");
        properties.setProperty("default.url", getJdbcConnection());
        properties.setProperty("default.user", "");
        properties.setProperty("default.password", "");
        ImpalaInterpreter t = new ImpalaInterpreter(properties);
        t.open();

        String sqlQuery = "select * from test_table";

        InterpreterResult interpreterResult = t.interpret(sqlQuery, interpreterContext);

        assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());
        assertEquals(InterpreterResult.Type.TABLE, interpreterResult.type());
        assertEquals("id\tname\na\ta_name\n", interpreterResult.message());
    }

    @Test
    public void concurrentSettingTest() {
        Properties properties = new Properties();
        properties.setProperty("zeppelin.jdbc.concurrent.use", "true");
        properties.setProperty("zeppelin.jdbc.concurrent.max_connection", "10");
        ImpalaInterpreter ImpalaInterpreter = new ImpalaInterpreter(properties);

        assertTrue(ImpalaInterpreter.isConcurrentExecution());
        assertEquals(10, ImpalaInterpreter.getMaxConcurrentConnection());

        Scheduler scheduler = ImpalaInterpreter.getScheduler();
        assertTrue(scheduler instanceof ParallelScheduler);

        properties.clear();
        properties.setProperty("zeppelin.jdbc.concurrent.use", "false");
        ImpalaInterpreter = new ImpalaInterpreter(properties);

        assertFalse(ImpalaInterpreter.isConcurrentExecution());

        scheduler = ImpalaInterpreter.getScheduler();
        assertTrue(scheduler instanceof FIFOScheduler);
    }

    @Test
    public void testAutoCompletion() throws SQLException, IOException {
        Properties properties = new Properties();
        properties.setProperty("common.max_count", "1000");
        properties.setProperty("common.max_retry", "3");
        properties.setProperty("default.driver", "org.apache.hive.jdbc.HiveDriver");
        properties.setProperty("default.url", getJdbcConnection());
        properties.setProperty("default.user", "");
        properties.setProperty("default.password", "");
        ImpalaInterpreter ImpalaInterpreter = new ImpalaInterpreter(properties);
        ImpalaInterpreter.open();

        ImpalaInterpreter.interpret("", interpreterContext);

        List<InterpreterCompletion> completionList = ImpalaInterpreter.completion("SEL", 0);

        InterpreterCompletion correctCompletionKeyword = new InterpreterCompletion("SELECT", "SELECT");

        assertEquals(2, completionList.size());
        assertEquals(true, completionList.contains(correctCompletionKeyword));
        assertEquals(0, ImpalaInterpreter.completion("SEL", 100).size());
    }

}
