package tech.ydb.importer.target;

/**
 * A unit of work for the writer pool.
 * Wraps an upload action that the writer thread executes.
 */
public class TaggedBatch {

    static final TaggedBatch POISON = new TaggedBatch(null);

    private final Runnable uploadAction;

    public TaggedBatch(Runnable uploadAction) {
        this.uploadAction = uploadAction;
    }

    public void execute() {
        uploadAction.run();
    }
}
