package tech.ydb.importer.integration;

import org.junit.jupiter.api.Assertions;
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

    @BeforeAll
    public static void init() {
        pg.start();
    }

    @Test
    public void conversion() {

    }

}
