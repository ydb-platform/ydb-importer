package tech.ydb.importer.config;

import java.io.File;
import org.junit.Assert;
import org.junit.Test;
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
        Assert.assertNotNull("missing source", ic.getSource());
        Assert.assertEquals(SourceType.ORACLE, ic.getSource().getType());
        Assert.assertNotNull("missing source URL", ic.getSource().getJdbcUrl());
        Assert.assertNotNull("missing source class", ic.getSource().getClassName());
        Assert.assertNotNull("missing source username", ic.getSource().getUserName());
        Assert.assertNotNull("missing source password", ic.getSource().getPassword());
        Assert.assertNotNull("missing target", ic.getTarget());
        Assert.assertEquals(TargetType.YDB, ic.getTarget().getType());
        Assert.assertNotNull("missing target script config", ic.getTarget().getScript());
        Assert.assertNotNull("missing target script filename", ic.getTarget().getScript().getFileName());
        Assert.assertFalse("empty target script filename",
                isBlank(ic.getTarget().getScript().getFileName()));
    }
    
}
