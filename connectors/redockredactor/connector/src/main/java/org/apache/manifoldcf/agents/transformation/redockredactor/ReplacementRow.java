/**
 * Copyright reDock Inc. 2020, All Rights Reserved
 */

package org.apache.manifoldcf.agents.transformation.redockredactor;

public class ReplacementRow {
    public String groupId;
    public String type;
    public String target;
    public String replacement;
    public String config;

    ReplacementRow(String groupId, String type, String target, String replacement, String config) {
        this.groupId = groupId;
        this.type = type;
        this.target = target;
        this.replacement = replacement;
        this.config = config;
    }
}
