package tech.ydb.importer.config;

import org.jdom2.Element;
import static tech.ydb.importer.config.JdomHelper.*;

/**
 *
 * @author zinal
 */
public class NameFilter {
    
    private final String value;
    private final boolean regexp;
    private final boolean upper;

    public NameFilter(String value, boolean regexp, boolean upper) {
        this.value = value;
        this.regexp = regexp;
        this.upper = upper;
    }
    
    public NameFilter(Element c) {
        this.value = getText(c);
        this.regexp = getBoolean(c, "regexp", false);
        this.upper = getBoolean(c, "upper", false);
    }

    public String getValue() {
        return value;
    }

    public boolean isRegexp() {
        return regexp;
    }

    public boolean isUpper() {
        return upper;
    }
    
}
