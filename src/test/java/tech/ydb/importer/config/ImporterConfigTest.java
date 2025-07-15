package tech.ydb.importer.config;

import java.io.File;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import static tech.ydb.importer.config.JdomHelper.*;

/**
 *
 * @author zinal
 */
public class ImporterConfigTest {

    @Test
    public void checkLoad() throws Exception {
        final ImporterConfig ic = new ImporterConfig(
                    readDocument( new File("scripts", "sample-oracle.xml") ));
        Assertions.assertNotNull(ic.getSource(), "missing source");
        Assertions.assertEquals(SourceType.ORACLE, ic.getSource().getType());
        Assertions.assertNotNull(ic.getSource().getJdbcUrl(), "missing source URL");
        Assertions.assertNotNull(ic.getSource().getClassName(), "missing source class");
        Assertions.assertNotNull(ic.getSource().getUserName(), "missing source username");
        Assertions.assertNotNull(ic.getSource().getPassword(), "missing source password");
        Assertions.assertNotNull(ic.getTarget(), "missing target");
        Assertions.assertEquals(TargetType.YDB, ic.getTarget().getType());
        Assertions.assertNotNull(ic.getTarget().getScript(), "missing target script config");
        Assertions.assertNotNull(ic.getTarget().getScript().getFileName(), "missing target script filename");
        Assertions.assertFalse(isBlank(ic.getTarget().getScript().getFileName()), "empty target script filename");
    }

}
