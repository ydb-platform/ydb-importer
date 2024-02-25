package tech.ydb.importer.config;

import org.jdom2.Element;

/**
 *
 * @author zinal
 */
public class TargetScript extends JdomHelper {

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
