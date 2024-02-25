package tech.ydb.importer;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import tech.ydb.core.Issue;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.importer.target.TargetTable;
import tech.ydb.table.Session;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.description.TableDescription;

/**
 * This tasks checks for table existence, drops existing tables if desired, or grabs the existing
 * table structure if dropping is not requested, then creates the table if it is missing.
 *
 * @author zinal
 */
public class CreateTableTask implements Callable<CreateTableTask.Out> {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(CreateTableTask.class);

    private final YdbImporter owner;
    private final TargetTable table;

    public CreateTableTask(YdbImporter owner, TargetTable table) {
        this.owner = owner;
        this.table = table;
    }

    @Override
    public Out call() throws Exception {
        try {
            final String fullName = owner.getTargetCP().getDatabase() + "/" + table.getFullName();
            LOG.debug("\tchecking full path: {}", fullName);
            SessionRetryContext retryCtx = owner.getTargetCP().getRetryCtx();
            Result<TableDescription> describeResult = retryCtx.supplyResult(
                    session -> session.describeTable(fullName)
            ).join();
            final boolean tableExists = describeResult.isSuccess();
            if (tableExists) {
                if (!owner.getConfig().getTarget().isReplaceExisting()) {
                    LOG.info("Table already exists: {}", table.getFullName());
                    return new Out(table, describeResult.getValue());
                }
                runSchemaOperation(
                        session -> session.dropTable(fullName),
                        "drop table problem"
                );
                LOG.info("Table dropped: {}", table.getFullName());
            }
            LOG.debug("\tcreating table: {} -> {}", table.getFullName(), table.getYqlScript());
            runSchemaOperation(
                    session -> session.executeSchemeQuery(table.getYqlScript()),
                    "create table problem"
            );
            LOG.info("Table created: {}", table.getFullName());
            return new Out(table, true);
        } catch (Throwable e) {
            LOG.error("Failed to create table `{}`", table.getFullName(), e);
            return new Out(table, false);
        }
    }

    private void runSchemaOperation(Function<Session, CompletableFuture<Status>> fn, String msg) {
        while (true) {
            Status status = owner.getTargetCP().getRetryCtx().supplyStatus(fn).join();
            if (status.isSuccess()) {
                return;
            }
            if (canRetry(status)) {
                LOG.debug("\t Retry schema operation due to error {}", status);
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextLong(1000L, 5000L));
                } catch (InterruptedException ix) {
                }
            } else {
                status.expectSuccess(msg);
            }
        }
    }

    private static boolean canRetry(Status status) {
        if (status == null) {
            return false;
        }
        Issue issue = null;
        if (status.getIssues().length > 0) {
            issue = status.getIssues()[0];
        }
        if (issue == null) {
            return false;
        }
        while (issue.getIssues().length > 0) {
            Issue tmp = issue.getIssues()[0];
            if (tmp == null) {
                break;
            }
            issue = tmp;
        }
        String msg = issue.getMessage();
        if (msg.contains("Request exceeded a limit ")) {
            return true;
        }
        return false;
    }

    public static final class Out {

        private final TargetTable table;
        private final boolean success;
        private final TableDescription existingTable;

        public Out(TargetTable table, boolean success) {
            this.table = table;
            this.success = success;
            this.existingTable = null;
        }

        public Out(TargetTable table, TableDescription existingTable) {
            this.table = table;
            this.success = true;
            this.existingTable = existingTable;
        }

        public TargetTable getTable() {
            return table;
        }

        public boolean isSuccess() {
            return success;
        }

        public TableDescription getExistingTable() {
            return existingTable;
        }

    }

}
