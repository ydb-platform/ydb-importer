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
    
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(TargetCP.class);

    private final GrpcTransport transport;
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
        this.database = transport.getDatabase();
        try {
            this.tableClient = TableClient.newClient(transport)
                    .sessionPoolSize(0, poolSize)
                    .build();
            this.retryCtx = SessionRetryContext.create(tableClient).build();
            this.transport = transport;
            transport = null; // to avoid closing below
        } finally {
            if (transport != null)
                transport.close();
        }
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
        if (tableClient != null) {
            try {
                tableClient.close();
            } catch(Exception ex) {
                LOG.warn("TableClient closing threw an exception", ex);
            }
        }
        if (transport != null) {
            try {
                transport.close();
            } catch(Exception ex) {
                LOG.warn("GrpcTransport closing threw an exception", ex);
            }
        }
    }

}
