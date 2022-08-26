package ydb.importer.target;

import com.yandex.ydb.auth.iam.CloudAuthHelper;
import com.yandex.ydb.core.grpc.GrpcTransport;
import com.yandex.ydb.table.SessionRetryContext;
import com.yandex.ydb.table.TableClient;
import com.yandex.ydb.table.rpc.grpc.GrpcTableRpc;
import ydb.importer.config.TargetConfig;

/**
 *
 * @author zinal
 */
public class TargetCP implements AutoCloseable {

    private final TableClient tableClient;
    private final SessionRetryContext retryCtx;
    private final String database;

    public TargetCP(TargetConfig config, int poolSize) {
        GrpcTransport.Builder builder = GrpcTransport
                .forConnectionString(config.getConnectionString());
        switch (config.getAuthMode()) {
            case ENV:
                builder = builder.withAuthProvider(
                        CloudAuthHelper.getAuthProviderFromEnviron());
                break;
            case NONE:
                break;
        }
        GrpcTransport transport = builder.build();
        GrpcTableRpc rpc = GrpcTableRpc.ownTransport(transport);
        this.tableClient = TableClient.newClient(rpc)
                .sessionPoolSize(0, poolSize)
                .build();
        this.database = transport.getDatabase();
        this.retryCtx = SessionRetryContext.create(tableClient).build();
    }

    public TableClient getTableClient() {
        return tableClient;
    }

    public SessionRetryContext getRetryCtx() {
        return retryCtx;
    }

    public String getDatabase() {
        return database;
    }

    @Override
    public void close() {
        tableClient.close();
    }

}
