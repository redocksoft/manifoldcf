/**
 * Copyright reDock Inc. 2020, All Rights Reserved
 */

package org.apache.manifoldcf.agents.transformation.redockredactor;

import org.apache.manifoldcf.core.database.BaseTable;
import org.apache.manifoldcf.core.interfaces.*;
import org.apache.manifoldcf.core.system.ManifoldCF;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ReplacementsManager extends BaseTable {
    // NOTE This must be in capital letters
    protected final static String DATABASE_TABLE_NAME = "REDOCK_REDACTOR_REPLACEMENTS_TABLE";

    protected final static String ID_FIELD = "id";
    protected final static String GROUP_ID_FIELD = "groupId";
    protected final static String TYPE_FIELD = "type";
    protected final static String TARGET_FIELD = "target";
    protected final static String REPLACEMENT_FIELD = "replacement";
    protected final static String CONFIG_FIELD = "config";

    /**
     * The overall table cache key
     */
    protected final static String TABLE_CACHE_KEY = "table-" + DATABASE_TABLE_NAME;
    /**
     * The prefix of the per-key cache key
     */
    protected final static String CACHE_KEY_PREFIX = TABLE_CACHE_KEY + "-";
    /**
     * The overall table cache key set
     */
    protected final static StringSet tableCacheKeySet = new StringSet(TABLE_CACHE_KEY);

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
        columnMap.put(ID_FIELD, new ColumnDescription("BIGINT", true, false, null, null, false));
        columnMap.put(GROUP_ID_FIELD, new ColumnDescription("VARCHAR(1024)", false, false, null, null, false));
        columnMap.put(TYPE_FIELD, new ColumnDescription("CHAR(1)", false, true, null, null, false));
        columnMap.put(TARGET_FIELD, new ColumnDescription("VARCHAR(1024)", false, true, null, null, false));
        columnMap.put(REPLACEMENT_FIELD, new ColumnDescription("VARCHAR(1024)", false, true, null, null, false));
        columnMap.put(CONFIG_FIELD, new ColumnDescription("VARCHAR(1024)", false, true, null, null, false));
        performCreate(columnMap, null);
        // Create an index
        performAddIndex(null, new IndexDescription(false, new String[]{GROUP_ID_FIELD}));
    }

    /**
     * Look up all the replacements matching a group
     */
    public ReplacementRow[] getReplacements(String groupId)
            throws ManifoldCFException {
        // We will cache this against the table as a whole, and also against the
        // values for the given group.  Any changes to either will invalidate it.
        StringSet cacheKeys = new StringSet(new String[]{TABLE_CACHE_KEY, makeCacheKey(groupId)});
        // Construct the parameters
        ArrayList params = new ArrayList();
        params.add(groupId);
        // Perform the query
        IResultSet set = performQuery("SELECT " + TYPE_FIELD + "," + TARGET_FIELD + "," + REPLACEMENT_FIELD + "," + CONFIG_FIELD + " FROM " + getTableName() +
                " WHERE " + GROUP_ID_FIELD + "=?", params, cacheKeys, null);
        // Assemble the results
        ReplacementRow[] results = new ReplacementRow[set.getRowCount()];
        int i = 0;
        while (i < results.length) {
            IResultRow row = set.getRow(i);
            results[i] = new ReplacementRow(groupId, (String) row.getValue(TYPE_FIELD), (String) row.getValue(TARGET_FIELD), (String) row.getValue(REPLACEMENT_FIELD), (String) row.getValue(CONFIG_FIELD));
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
        fields.put(ID_FIELD, new Long(IDFactory.make(threadContext)));
        fields.put(GROUP_ID_FIELD, replacement.groupId);
        fields.put(TYPE_FIELD, replacement.type);
        fields.put(TARGET_FIELD, replacement.target);
        fields.put(REPLACEMENT_FIELD, replacement.replacement);
        fields.put(CONFIG_FIELD, replacement.config);
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
        StringSet invalidationKeys = new StringSet(new String[]{TABLE_CACHE_KEY});
        // Perform the delete
        performDelete("WHERE " + GROUP_ID_FIELD + "=?", params, invalidationKeys);
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
        return CACHE_KEY_PREFIX + id;
    }
}
