package tech.ydb.importer.integration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.PostgreSQLContainer;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author mzinal
 */
public class PgToYdbTest {

    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    private static PostgreSQLContainer pg = new PostgreSQLContainer("postgres:17.5")
            .withDatabaseName("integration-tests-db")
            .withUsername("sa")
            .withPassword("sa");

    private Connection connection = null;

    @BeforeAll
    public static void init() {
        pg.start();
    }

    @AfterAll
    public static void done() {
        pg.stop();
    }

    @Test
    public void conversion() throws Exception {
        Class.forName(pg.getDriverClassName());
        connection = DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());

        List<InputAny> inputs = InputAny.getInputs();
        inputs.forEach(this::runCreate);
        inputs.forEach(this::runInsert);

        inputs.forEach(this::runDrop);
    }

    private void runCreate(InputAny ia) {
        runDdl(ia.getCreate());
    }

    private void runDrop(InputAny ia) {
        runDdl(ia.getDrop());
    }

    private void runInsert(InputAny ia) {
        runDml(ia.getInsert());
    }

    private void runDdl(List<String> commands) {
        try {
            connection.setAutoCommit(true);
            Statement stmt = connection.createStatement();
            for (String sql : commands) {
                System.out.println("DDL> " + sql);
                stmt.execute(sql);
            }
            connection.setAutoCommit(false);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void runDml(List<String> commands) {
        try {
            connection.setAutoCommit(false);
            Statement stmt = connection.createStatement();
            for (String sql : commands) {
                System.out.println("DDL> " + sql);
                stmt.execute(sql);
            }
            connection.commit();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

}
