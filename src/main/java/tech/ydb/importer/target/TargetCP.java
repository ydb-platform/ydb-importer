package tech.ydb.importer.target;

import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.auth.StaticCredentials;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;

import tech.ydb.importer.config.TargetConfig;

/**
 *
 * @author zinal
 */
public class TargetCP implements AutoCloseable {

    private final TableClient tableClient;
    private final SessionRetryContext retryCtx;
    private final String database;

    public TargetCP(TargetConfig config, int poolSize) {
        GrpcTransportBuilder builder = GrpcTransport
                .forConnectionString(config.getConnectionString());
        switch (config.getAuthMode()) {
            case ENV:
                builder = builder.withAuthProvider(
                        CloudAuthHelper.getAuthProviderFromEnviron());
                break;
            case STATIC:
                builder = builder.withAuthProvider(
                    new StaticCredentials(config.getStaticLogin(), config.getStaticPassword()));
                break;
            case NONE:
                break;
        }
        GrpcTransport transport = builder.build();
        this.tableClient = TableClient.newClient(transport)
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
