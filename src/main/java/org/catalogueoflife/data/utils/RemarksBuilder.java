package org.catalogueoflife.data.utils;

import com.google.common.base.Strings;

public class RemarksBuilder {
    private StringBuilder sb = new StringBuilder();

    /**
     * @param remark to append. If not trailed by an . or ! an full stop will be added
     */
    public void append(String remark) {
        if (!Strings.isNullOrEmpty(remark)) {
            if (sb.length() > 0) {
                sb.append(" ");
            }
            var val = remark.trim();
            sb.append(val);
            if (!val.endsWith(".") && !val.endsWith("!")) {
                sb.append(".");
            }
        }
    }

    public boolean isEmpty() {
        return sb.isEmpty();
    }

    public String toString() {
        return sb.length() > 0 ? sb.toString() : null;
    }
}
