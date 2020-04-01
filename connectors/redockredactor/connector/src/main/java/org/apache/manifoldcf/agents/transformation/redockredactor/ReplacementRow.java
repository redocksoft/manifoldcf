package org.apache.manifoldcf.agents.transformation.redockredactor;

public class ReplacementRow {
    public String groupId;
    public String target;
    public String replacement;

    ReplacementRow(String groupId, String target, String replacement) {
        this.groupId = groupId;
        this.target = target;
        this.replacement = replacement;
    }
}
