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

import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.LoopbackClientSideMetadataProvider;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Basic client testing
 *
 * @author enrico.olivelli
 */
public class BatchTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testBatch() throws Exception {
        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));) {
                client.setClientSideMetadataProvider(new LoopbackClientSideMetadataProvider(server));
                try (HerdDBDataSource dataSource = new HerdDBDataSource(client);
                        Connection con = dataSource.getConnection();
                        Statement create = con.createStatement();
                        PreparedStatement statement = con.prepareStatement("INSERT INTO mytable (name) values(?)");) {
                    create.execute("CREATE TABLE mytable (n1 int primary key auto_increment, name string)");

                    for (int i = 0; i < 100; i++) {
                        statement.setString(1, "v" + i);
                        statement.addBatch();
                    }
                    int[] results = statement.executeBatch();
                    for (int i = 0; i < 100; i++) {
                        assertEquals(1, results[i]);
                    }

                    try (ResultSet rs = statement.executeQuery("SELECT * FROM mytable ORDER BY n1")) {
                        int count = 0;
                        while (rs.next()) {
                            assertEquals("v" + count, rs.getString("name"));
                            assertEquals(count+1, rs.getInt("n1"));
                            count++;
                        }
                        assertEquals(100, count);
                    }
                }
            }
        }

    }

}