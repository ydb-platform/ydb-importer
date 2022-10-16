package tech.ydb.importer.config;

import java.io.File;
import java.io.FileInputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.jdom2.located.Located;
import org.jdom2.located.LocatedJDOMFactory;

/**
 *
 * @author zinal
 */
public class JdomHelper {
    
    public static final String ATTR_FILE_NAME = "jdom-private-file-name";

    public static int length(final CharSequence cs) {
        return cs == null ? 0 : cs.length();
    }

    public static boolean isBlank(final CharSequence cs) {
        final int strLen = length(cs);
        if (strLen == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(cs.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static String getFileName(Element el) {
        String fname = null;
        Element cur = el;
        while (fname == null && cur != null) {
            fname = cur.getAttributeValue(ATTR_FILE_NAME);
            cur = cur.getParentElement();
        }
        if (fname != null)
            return fname;
        return "<unknown>.xml";
    }
    
    public static Element readDocument(File file) throws Exception {
        try (FileInputStream fis = new FileInputStream(file)) {
            Element root = new SAXBuilder(null, null, new LocatedJDOMFactory())
                            . build(fis) . detachRootElement();
            root.setAttribute(ATTR_FILE_NAME, file.getPath());
            return root;
        }
    }
    
    public static Element readDocument(String fileName) throws Exception {
        return readDocument(new File(fileName));
    }

    public static String getPosition(Element el) {
        if (el instanceof Located) {
            Located l = (Located) el;
            return "line " + String.valueOf(l.getLine())
                    + ", column " + String.valueOf(l.getColumn());
        } else {
            return "position unknown";
        }
    }

    public static RuntimeException raise(Element el, CharSequence message) {
        return new RuntimeException("Parse error at "
                + getPosition(el) + " in file [" + getFileName(el) + "]: "
                + message
        );
    }

    public static RuntimeException raise(Element el, CharSequence message, 
            Throwable cause) {
        return new RuntimeException("Parse error at "
                + getPosition(el) + " in file [" + getFileName(el) + "]: "
                + message, cause
        );
    }

    public static RuntimeException raiseMissing(Element el, String name) {
        return raise(el, "Missing attribute '" + name + "' for tag '" + el.getName() + "'");
    }

    public static RuntimeException raiseIllegal(Element el, String name, String value) {
        if (isBlank(name)) {
            return raise(el, "Illegal value in tag '" + el.getName() + "': [" + value + "]");
        }
        return raise(el, "Illegal value for attribute '" + name + "' in tag '"
                + el.getName() + "': [" + value + "]");
    }
    
    public static RuntimeException raiseIllegal(Element el, String name) {
        return raiseIllegal(el, name, el.getAttributeValue(name));
    }
    
    public static Element getOneChild(Element el, String name) {
        List<Element> children = el.getChildren(name);
        Iterator<Element> it = (children==null) ? null : children.iterator();
        if (it==null || !it.hasNext()) {
            return null;
        }
        Element ret = it.next();
        if (it.hasNext()) {
            throw raise(el, "Multiple subtags '" + name 
                    + "' for tag '" + el.getName() + "'");
        }
        return ret;
    }
    
    public static Element getSingleChild(Element el, String name) {
        List<Element> children = el.getChildren(name);
        Iterator<Element> it = (children==null) ? null : children.iterator();
        if (it==null || !it.hasNext()) {
            throw raise(el, "Missing subtag '" + name 
                    + "' for tag '" + el.getName() + "'");
        }
        Element ret = it.next();
        if (it.hasNext()) {
            throw raise(el, "Multiple subtags '" + name 
                    + "' for tag '" + el.getName() + "'");
        }
        return ret;
    }
    
    public static List<Element> getChildren(Element el, String name) {
        List<Element> children = el.getChildren(name);
        if (children==null || children.isEmpty())
            return Collections.emptyList();
        return children;
    }
    
    /**
     * Retrieve at least one sub-element with the specified name.
     * @param el Current element
     * @param name Names of sub-elements
     * @return The list with at least one sub-element, 
     *      exception is thrown otherwise
     */
    public static List<Element> getSomeChildren(Element el, String name) {
        List<Element> children = el.getChildren(name);
        if (children==null || children.isEmpty()) {
            throw raise(el, "Missing subtags '" + name 
                    + "' for tag '" + el.getName() + "'");
        }
        return children;
    }
    
    public static String getText(Element el, boolean allowBlank) {
        if (el==null) {
            if (allowBlank)
                return null;
            throw new IllegalArgumentException("JdomHelper.getText(null, false)");
        }
        String value = el.getText();
        if (allowBlank)
            return value;
        if (isBlank(value))
            throw raise(el, "Missing value for tag '" + el.getName() + "'");
        return value;
    }

    public static String getText(Element el) {
        return getText(el, false);
    }
    
    public static String getText(Element el, String childName) {
        return getText(getSingleChild(el, childName), false);
    }
    
    public static String getText(Element el, String childName, String defval) {
        String value =  getText(getOneChild(el, childName), true);
        if (isBlank(value))
            return defval;
        return value;
    }
    
    public static String getAttr(Element el, String name, String defval) {
        String value = el.getAttributeValue(name);
        if (isBlank(value))
            return defval;
        return value;
    }

    public static String getAttr(Element el, String name) {
        String value = el.getAttributeValue(name);
        if (isBlank(value))
            throw raiseMissing(el, name);
        return value;
    }

    public static boolean parseBoolean(Element el, String name, String value) {
        if (value==null)
            throw raiseMissing(el, name);
        value = value.trim();
        if (value.length()==0)
            throw raiseMissing(el, name);
        value = value.substring(0, 1);
        if ( "Y".equalsIgnoreCase(value)
                || "1".equalsIgnoreCase(value)
                || "T".equalsIgnoreCase(value)
                || "Д".equalsIgnoreCase(value)
                || "И".equalsIgnoreCase(value) )
            return true;
        if ( "N".equalsIgnoreCase(value)
                || "0".equalsIgnoreCase(value)
                || "F".equalsIgnoreCase(value)
                || "Н".equalsIgnoreCase(value)
                || "Л".equalsIgnoreCase(value) )
            return false;
        throw raiseIllegal(el, name, value);
    }

    public static boolean getBoolean(Element el, String name) {
        return parseBoolean(el, name, el.getAttributeValue(name));
    }
    
    public static boolean getBoolean(Element el, String name, boolean defval) {
        String value = el.getAttributeValue(name);
        if (value==null)
            return defval;
        value = value.trim();
        if (value.length()==0)
            return defval;
        return parseBoolean(el, name, value);
    }
    
    
    public static int getInt(Element el) {
        String vs = getText(el);
        try {
            return Integer.parseInt(vs.trim());
        } catch(NumberFormatException nfe) {
            throw raiseIllegal(el, null, vs);
        }
    }

    public static int getInt(Element el, String name) {
        String vs = el.getAttributeValue(name);
        if (isBlank(vs))
            throw raiseMissing(el, name);
        try {
            return Integer.parseInt(vs.trim());
        } catch(NumberFormatException nfe) {
            throw raiseIllegal(el, name);
        }
    }

    public static int getInt(Element el, String name, int defval) {
        String vs = el.getAttributeValue(name);
        if (isBlank(vs))
            return defval;
        try {
            return Integer.parseInt(vs.trim());
        } catch(NumberFormatException nfe) {
            throw raiseIllegal(el, name);
        }
    }

    public static long getLong(Element el, String name) {
        String vs = el.getAttributeValue(name);
        if (isBlank(vs))
            throw raiseMissing(el, name);
        try {
            return Long.parseLong(vs.trim());
        } catch(NumberFormatException nfe) {
            throw raiseIllegal(el, name);
        }
    }

    public static long getLong(Element el, String name, long defval) {
        String vs = el.getAttributeValue(name);
        if (isBlank(vs))
            return defval;
        try {
            return Long.parseLong(vs.trim());
        } catch(NumberFormatException nfe) {
            throw raiseIllegal(el, name);
        }
    }
}
