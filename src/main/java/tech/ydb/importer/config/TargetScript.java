package tech.ydb.importer.config;

import org.jdom2.Element;
import static tech.ydb.importer.config.JdomHelper.*;

/**
 *
 * @author zinal
 */
public class TargetScript {

    private String fileName;

    public TargetScript() {
    }

    public TargetScript(Element c) {
        this.fileName = getText(c);
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

}
