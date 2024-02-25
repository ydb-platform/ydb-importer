package tech.ydb.importer.source;

/**
 * Column information, as part of source table metadata.
 *
 * @author mzinal
 */
public class ColumnInfo {

    private final String name;
    private final String destinationName;
    private int position;
    private int sqlType;
    private int sqlPrecision;
    private int sqlScale;
    private boolean blobAsObject;

    public ColumnInfo(String name) {
        this.name = (name == null) ? "" : name;
        this.destinationName = safeYdbColumnName(name);
        this.position = -1;
        this.sqlType = java.sql.Types.VARCHAR;
        this.sqlPrecision = 0;
        this.sqlScale = 0;
        this.blobAsObject = false;
    }

    public String getName() {
        return name;
    }

    public String getDestinationName() {
        return destinationName;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public int getSqlType() {
        return sqlType;
    }

    public void setSqlType(int sqlType) {
        this.sqlType = sqlType;
    }

    public int getSqlPrecision() {
        return sqlPrecision;
    }

    public void setSqlPrecision(int sqlPrecision) {
        this.sqlPrecision = sqlPrecision;
    }

    public int getSqlScale() {
        return sqlScale;
    }

    public void setSqlScale(int sqlScale) {
        this.sqlScale = sqlScale;
    }

    public boolean isBlobAsObject() {
        return blobAsObject;
    }

    public void setBlobAsObject(boolean blobAsObject) {
        this.blobAsObject = blobAsObject;
    }

    public static boolean isBlob(int sqlType) {
        switch (sqlType) {
            case java.sql.Types.BLOB:
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.SQLXML:
                return true;
            default:
                return false;
        }
    }

    public boolean isBlob() {
        return isBlob(sqlType);
    }

    public static String safeYdbColumnName(String name) {
        if (name == null || name.length() == 0) {
            return "";
        }
        name = name.replace(' ', '_');
        name = name.replace('.', '_');
        name = name.replace('/', '_');
        name = name.replace('`', '_');
        return name;
    }

}
