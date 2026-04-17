package tech.ydb.importer.integration.verification;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@FunctionalInterface
public interface BlobInserter {

    void bind(Connection conn, PreparedStatement ps, int index,
              byte[] blob) throws SQLException;

    BlobInserter DEFAULT = (conn, ps, index, blob) -> ps.setBytes(index, blob);
}
