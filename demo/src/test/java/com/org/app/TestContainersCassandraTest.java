package com.org.app;

import static org.junit.Assert.assertEquals;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.CassandraContainer;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;

/**
 * Unit tests example using TestContainers Cassandra.
 */
public class TestContainersCassandraTest {
    private CqlSession session;
    private final CassandraContainer<?> cassandra = new CassandraContainer<>("cassandra:3.11.2");
    private static final String KEYSPACE_NAME = "test_keyspace";

    @Before
    public void setUp() throws Exception {
        this.cassandra.start();
        session = CqlSession
                .builder()
                .addContactPoint(this.cassandra.getContactPoint())
                .withLocalDatacenter(this.cassandra.getLocalDatacenter())
                .build();
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE_NAME
                + " WITH replication = {'class': 'SimpleStrategy','replication_factor' : 1};");
    }

    @Test
    public void givenCassandraInstance_whenStarted_thenContainerIsUp() {
        assertEquals(this.cassandra.isRunning(), true);
    }

    @Test
    public void givenCassandraInstance_whenContainerUp_thenKeyspaceCreated() {
        ResultSet result = session
                .execute("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = '" + KEYSPACE_NAME + "';");
        String keyspaceName = result.iterator().next().getString("keyspace_name");
        assertEquals(KEYSPACE_NAME, keyspaceName);
    }

    @Test
    public void givenCassandraInstance_whenContainerUp_thenTableCreatedAndDataInserted() {
        session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE_NAME + ".person (id varchar, name varchar, PRIMARY KEY(id));");
        session.execute("INSERT INTO " + KEYSPACE_NAME + ".person (id, name) VALUES ('-1', 'Troy');");
        session.execute("INSERT INTO " + KEYSPACE_NAME + ".person (id, name) VALUES ('1', 'Abed');");

        ResultSet result = session
                .execute("SELECT * FROM " + KEYSPACE_NAME + ".person WHERE id = '1';");
        String resultName = result.iterator().next().getString("name");
        assertEquals(resultName, "Abed");
    }


    @After
    public void tearDown() {
        try {
            this.session.close();
            this.cassandra.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
