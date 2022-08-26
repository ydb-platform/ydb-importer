package ydb.importer.source;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import ydb.importer.config.NameFilter;

/**
 * Check the filters (inclusion and exclusion) over the schema or table name.
 * @author zinal
 */
public class NameChecker {
    
    private final Item include;
    private final Item exclude;

    public NameChecker(List<NameFilter> include, List<NameFilter> exclude) {
        this.include = new Item(include);
        this.exclude = new Item(exclude);
    }
    
    public boolean decide(String value) {
        if (!include.isEmpty() && !itemMatches(value, include))
            return false;
        if (!exclude.isEmpty() && itemMatches(value, exclude))
            return false;
        return true;
    }

    private boolean itemMatches(String value, Item item) {
        for (int i=0; i<item.f.size(); ++i) {
            if (item.matches(i, value))
                return true;
        }
        return false;
    }
    
    public static final class Item {
        public final List<NameFilter> f;
        public final List<Pattern> p;
        
        public Item(List<NameFilter> f) {
            this.f = (f==null) ? Collections.emptyList() : f;
            this.p = new ArrayList<>(this.f.size());
            for (int i=0; i<this.f.size(); ++i)
                this.p.add(null);
        }
        
        public boolean isEmpty() {
            return f.isEmpty();
        }
        
        public boolean matches(int pos, String value) {
            NameFilter nf = f.get(pos);
            if (nf==null)
                return false;
            if (nf.isRegexp()) {
                if (nf.isUpper())
                    value = value.toUpperCase();
                Pattern np = p.get(pos);
                if (np==null) {
                    String patt = nf.getValue();
                    if (nf.isUpper())
                        patt = patt.toUpperCase();
                    np = Pattern.compile(patt);
                    p.set(pos, np);
                }
                return np.matcher(value).matches();
            } else {
                String cmp = nf.getValue();
                if (cmp==null)
                    cmp = "";
                if (nf.isUpper())
                    return cmp.equalsIgnoreCase(value);
                return cmp.equals(value);
            }
        }
    }
    
}
