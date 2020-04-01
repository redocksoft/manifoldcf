package org.apache.manifoldcf.agents.transformation.redockredactor;

import org.apache.manifoldcf.core.interfaces.*;
import org.apache.manifoldcf.core.system.ManifoldCF;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ReplacementsManager extends org.apache.manifoldcf.core.database.BaseTable {
    // NOTE This must be in capital letters
    protected final static String DATABASE_TABLE_NAME = "REDOCK_REDACTOR_REPLACEMENTS_TABLE";

    protected final static String idField = "id";
    protected final static String groupIdField = "groupId";
    protected final static String targetField = "target";
    protected final static String replacementField = "replacement";

    /**
     * The overall table cache key
     */
    protected final static String TABLE_CACHEKEY = "table-" + DATABASE_TABLE_NAME;
    /**
     * The prefix of the per-key cache key
     */
    protected final static String CACHEKEY_PREFIX = TABLE_CACHEKEY + "-";
    /**
     * The overall table cache key set
     */
    protected final static StringSet tableCacheKeySet = new StringSet(TABLE_CACHEKEY);

    /**
     * The thread context
     */
    protected IThreadContext threadContext;

    /**
     * Constructor
     */
    public ReplacementsManager(IThreadContext threadContext)
            throws ManifoldCFException {
        super(DBInterfaceFactory.make(threadContext,
                ManifoldCF.getMasterDatabaseName(),
                ManifoldCF.getMasterDatabaseUsername(),
                ManifoldCF.getMasterDatabasePassword()),
                DATABASE_TABLE_NAME);
        this.threadContext = threadContext;
    }

    /**
     * Initialize the database table
     */
    public void initialize()
            throws ManifoldCFException {
        StringSet tables = dbInterface.getAllTables(null, null);

        if (tables.contains(DATABASE_TABLE_NAME)) {
            return;
        }

        // Create the table
        Map columnMap = new HashMap();
        columnMap.put(idField, new ColumnDescription("BIGINT", true, false, null, null, false));
        columnMap.put(groupIdField, new ColumnDescription("VARCHAR(255)", false, false, null, null, false));
        columnMap.put(targetField, new ColumnDescription("VARCHAR(255)", false, true, null, null, false));
        columnMap.put(replacementField, new ColumnDescription("VARCHAR(255)", false, true, null, null, false));
        performCreate(columnMap, null);
        // Create an index
        performAddIndex(null, new IndexDescription(false, new String[]{groupIdField}));
    }

    /**
     * Look up all the replacements matching a group
     */
    public ReplacementRow[] getReplacements(String groupId)
            throws ManifoldCFException {
        // We will cache this against the table as a whole, and also against the
        // values for the given group.  Any changes to either will invalidate it.
        StringSet cacheKeys = new StringSet(new String[]{TABLE_CACHEKEY, makeCacheKey(groupId)});
        // Construct the parameters
        ArrayList params = new ArrayList();
        params.add(groupId);
        // Perform the query
        IResultSet set = performQuery("SELECT " + targetField + "," + replacementField + " FROM " + getTableName() +
                " WHERE " + groupIdField + "=?", params, cacheKeys, null);
        // Assemble the results
        ReplacementRow[] results = new ReplacementRow[set.getRowCount()];
        int i = 0;
        while (i < results.length) {
            IResultRow row = set.getRow(i);
            results[i] = new ReplacementRow(groupId, (String) row.getValue(targetField), (String) row.getValue(replacementField));
            i++;
        }
        return results;
    }

    /**
     * Add a ReplacementRow
     */
    public void addReplacement(ReplacementRow replacement)
            throws ManifoldCFException {
        // Prepare the fields
        Map fields = new HashMap();
        fields.put(idField, IDFactory.make(threadContext));
        fields.put(groupIdField, replacement.groupId);
        fields.put(targetField, replacement.target);
        fields.put(replacementField, replacement.replacement);
        // Prepare the invalidation keys
        StringSet invalidationKeys = new StringSet(new String[]{makeCacheKey(replacement.groupId)});
        performInsert(fields, invalidationKeys);
    }

    /**
     * Delete all rows that have a given group
     */
    public void deleteGroup(String groupId)
            throws ManifoldCFException {
        if (groupId == null || groupId.isEmpty()) {
            return;
        }

        // Prepare the parameters
        ArrayList params = new ArrayList();
        params.add(groupId);
        // Prepare the invalidation keys
        StringSet invalidationKeys = new StringSet(new String[]{TABLE_CACHEKEY});
        // Perform the delete
        performDelete("WHERE " + groupIdField + "=?", params, invalidationKeys);
    }

    /**
     * Delete the database table
     */
    public void destroy()
            throws ManifoldCFException {
        performDrop(tableCacheKeySet);
    }

    /**
     * Construct a cache key for the given lookup key
     */
    protected static String makeCacheKey(String id) {
        return CACHEKEY_PREFIX + id;
    }
}
