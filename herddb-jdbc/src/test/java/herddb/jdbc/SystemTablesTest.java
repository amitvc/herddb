/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */
package herddb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.*;
import java.util.*;

import herddb.model.StatementExecutionException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.model.TableSpace;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.StaticClientSideMetadataProvider;

/**
 * Basic client testing
 *
 * @author enrico.olivelli
 */
public class SystemTablesTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try (BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client);
                     Connection con = dataSource.getConnection();
                     Statement statement = con.createStatement();) {
                    statement.execute("CREATE TABLE mytable (key string primary key, name string)");
                    statement.execute("CREATE INDEX mytableindex ON mytable(name)");
                    statement.execute("CREATE TABLE mytable2 (n2 int primary key auto_increment, name string, ts timestamp)");

                    try (ResultSet rs = statement.executeQuery("SELECT * FROM SYSTABLES")) {

                        Set<String> tables = new HashSet<>();
                        while (rs.next()) {
                            String name = rs.getString("table_name");
                            tables.add(name);
                        }
                        assertTrue(tables.contains("mytable"));
                        assertTrue(tables.contains("mytable2"));

                    }

                    DatabaseMetaData metaData = con.getMetaData();
                    try (ResultSet rs = metaData.getTables(null, null, null, null)) {
                        List<List<String>> records = new ArrayList<>();
                        while (rs.next()) {
                            List<String> record = new ArrayList<>();
                            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                                String value = rs.getString(i + 1);
                                record.add(value);
                            }
                            records.add(record);
                        }
                        assertTrue(records.stream().filter(s -> s.contains("mytable")).findAny().isPresent());
                        assertTrue(records.stream().filter(s -> s.contains("mytable2")).findAny().isPresent());
                        assertTrue(records.stream().filter(s -> s.contains("systables")).findAny().isPresent());
                        assertTrue(records.stream().filter(s -> s.contains("syscolumns")).findAny().isPresent());
                        assertTrue(records.stream().filter(s -> s.contains("sysstatements")).findAny().isPresent());

                    }

                    try (ResultSet rs = metaData.getTables(null, null, "m%table", null)) {
                        List<List<String>> records = new ArrayList<>();
                        while (rs.next()) {
                            List<String> record = new ArrayList<>();
                            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                                String value = rs.getString(i + 1);
                                record.add(value);
                            }
                            records.add(record);
                        }
                        assertTrue(records.stream().filter(s -> s.contains("mytable")).findAny().isPresent());
                        assertEquals(1, records.size());
                    }
                    try (ResultSet rs = metaData.getTables(null, null, "m_table%", null)) {
                        List<List<String>> records = new ArrayList<>();
                        while (rs.next()) {
                            List<String> record = new ArrayList<>();
                            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                                String value = rs.getString(i + 1);
                                record.add(value);
                            }
                            records.add(record);
                        }
                        assertTrue(records.stream().filter(s -> s.contains("mytable")).findAny().isPresent());
                        assertTrue(records.stream().filter(s -> s.contains("mytable2")).findAny().isPresent());
                        assertEquals(2, records.size());
                    }

                    try (ResultSet rs = metaData.getColumns(null, null, "mytable2", null)) {
                        List<List<String>> records = new ArrayList<>();
                        while (rs.next()) {
                            List<String> record = new ArrayList<>();
                            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                                String value = rs.getString(i + 1);
                                record.add(value);
                            }
                            records.add(record);
                        }
                        assertEquals(3, records.size());
                        assertTrue(records.stream().filter(s -> s.contains("n2")
                                && s.contains("integer")
                        ).findAny().isPresent());
                        assertTrue(records.stream().filter(s
                                -> s.contains("name") && s.contains("string")
                        ).findAny().isPresent());
                        assertTrue(records.stream().filter(s
                                -> s.contains("ts") && s.contains("timestamp")
                        ).findAny().isPresent());
                    }

                    try (ResultSet rs = metaData.getIndexInfo(null, null, "mytable", false, false)) {

                        List<List<String>> records = new ArrayList<>();
                        while (rs.next()) {
                            List<String> record = new ArrayList<>();
                            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                                String value = rs.getString(i + 1);
                                record.add(value);
                            }
                            records.add(record);
                        }
                        assertEquals(2, records.size()); // pk + secondary index
                    }

                    try (ResultSet rs = metaData.getIndexInfo(null, null, "mytable", true, false)) {

                        List<List<String>> records = new ArrayList<>();
                        while (rs.next()) {
                            List<String> record = new ArrayList<>();
                            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                                String value = rs.getString(i + 1);
                                record.add(value);
                            }
                            records.add(record);
                        }
                        assertEquals(1, records.size()); // only pk
                    }

                    try (ResultSet rs = metaData.getIndexInfo(null, null, null, true, false)) {
                        List<List<String>> records = new ArrayList<>();
                        while (rs.next()) {
                            List<String> record = new ArrayList<>();
                            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                                String value = rs.getString(i + 1);
                                record.add(value);
                            }
                            records.add(record);
                        }
                        // this is to be incremented at every new systable
                        assertEquals(21, records.size());
                    }
                    try (ResultSet rs = metaData.getSchemas()) {
                        List<List<String>> records = new ArrayList<>();
                        while (rs.next()) {
                            List<String> record = new ArrayList<>();
                            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                                String value = rs.getString(i + 1);
                                record.add(value);
                            }
                            records.add(record);
                        }
                        assertTrue(records.stream().filter(s -> s.contains(TableSpace.DEFAULT)).findAny().isPresent());
                        assertEquals(1, records.size());

                    }

                    try (ResultSet rs = metaData.getSchemas(null, "%" + TableSpace.DEFAULT.substring(2))) {
                        List<List<String>> records = new ArrayList<>();
                        while (rs.next()) {
                            List<String> record = new ArrayList<>();
                            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                                String value = rs.getString(i + 1);
                                record.add(value);
                            }
                            records.add(record);
                        }
                        assertTrue(records.stream().filter(s -> s.contains(TableSpace.DEFAULT)).findAny().isPresent());
                        assertEquals(1, records.size());

                    }
                    try (ResultSet rs = metaData.getSchemas(null, "no")) {
                        List<List<String>> records = new ArrayList<>();
                        assertFalse(rs.next());

                    }
                }
            }
        }
    }

    @Test
    public void ensureWeThrowExceptionForNonSupportedNotNullTypes_Double() throws Exception {
        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try {
                    BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client);
                    Connection con = dataSource.getConnection();
                    Statement statement = con.createStatement();
                    statement.execute("CREATE TABLE mytable (key string primary key, name string,d1 double not null)");
                } catch (SQLException ex) {
                    assertTrue(ex.getMessage().contains("StatementExecutionException"));
                    assertTrue(ex.getMessage().contains("Not null constraints not supported for column type 6"));
                }
            }
        }
    }

    @Test
    public void ensureWeThrowAppropriateExceptionWhenNotNullConstraintViolated_AddBatch_ExecuteBatch() throws Exception {
        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try {
                    BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client);
                    Connection con = dataSource.getConnection();
                    Statement statement = con.createStatement();
                    statement.execute("CREATE TABLE mytable (n1 int primary key auto_increment, name string not null)");
                    PreparedStatement insertPs = con.prepareStatement("INSERT INTO mytable (name) values(?)");
                    for (int i = 0; i < 10; i++) {
                        insertPs.setString(1, null);
                        insertPs.addBatch();
                    }
                    insertPs.executeBatch();
                } catch (SQLException ex) {
                    assertTrue(ex.getMessage().contains("StatementExecutionException"));
                    assertTrue(ex.getMessage().contains("Cannot have null value in non null type string not null"));
                }
            }
        }
    }

    @Test
    public void ensureMetaDataWorksForNotNullConstraints() throws Exception {
        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try (BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client);
                     Connection con = dataSource.getConnection();
                     Statement statement = con.createStatement();) {
                     statement.execute("CREATE TABLE mytable (key string primary key, name string)");
                     statement.execute("CREATE TABLE mytable2 (n2 int primary key auto_increment, name string not null, ts timestamp)");
                     DatabaseMetaData metaData = con.getMetaData();
                     try(ResultSet rs = metaData.getColumns(null, null, "mytable", null);) {
                         Map<String, Map<String, String>> records = new HashMap<>();

                         while (rs.next()) {
                             Map<String, String> record = new HashMap<>();
                             record.put("DATA_TYPE", rs.getString("DATA_TYPE"));
                             record.put("IS_NULLABLE", rs.getString("IS_NULLABLE"));
                             records.put(rs.getString("COLUMN_NAME"), record);
                         }
                         assertTrue(records.size() == 2);
                         assertTrue(records.get("key").get("DATA_TYPE").equalsIgnoreCase("string") && records.get("key").get("IS_NULLABLE").equalsIgnoreCase("NO"));
                         assertTrue(records.get("name").get("DATA_TYPE").equalsIgnoreCase("string") && records.get("name").get("IS_NULLABLE").equalsIgnoreCase("YES"));
                     }

                    try(ResultSet rs = metaData.getColumns(null, null, "mytable2", null);) {
                        Map<String, Map<String, String>> records = new HashMap<>();

                        while (rs.next()) {
                            Map<String, String> record = new HashMap<>();
                            record.put("DATA_TYPE", rs.getString("DATA_TYPE"));
                            record.put("IS_NULLABLE", rs.getString("IS_NULLABLE"));
                            records.put(rs.getString("COLUMN_NAME"), record);
                        }
                        assertTrue(records.size() == 3);
                        assertTrue(records.get("n2").get("DATA_TYPE").equalsIgnoreCase("integer") && records.get("n2").get("IS_NULLABLE").equalsIgnoreCase("NO"));
                        assertTrue(records.get("name").get("DATA_TYPE").equalsIgnoreCase("string not null") && records.get("name").get("IS_NULLABLE").equalsIgnoreCase("NO"));
                        assertTrue(records.get("ts").get("DATA_TYPE").equalsIgnoreCase("timestamp") && records.get("ts").get("IS_NULLABLE").equalsIgnoreCase("YES"));
                    }
                }
            }
        }
    }
}