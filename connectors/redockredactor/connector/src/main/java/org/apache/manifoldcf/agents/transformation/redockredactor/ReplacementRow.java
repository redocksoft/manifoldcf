/**
 * Copyright reDock Inc. 2020, All Rights Reserved
 */

package org.apache.manifoldcf.agents.transformation.redockredactor;

public class ReplacementRow {
    public String groupId;
    public String type;
    public String target;
    public String replacement;

    ReplacementRow(String groupId, String type, String target, String replacement) {
        this.groupId = groupId;
        this.type = type;
        this.target = target;
        this.replacement = replacement;
    }
}
