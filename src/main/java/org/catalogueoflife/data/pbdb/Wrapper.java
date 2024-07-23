package org.catalogueoflife.data.pbdb;

import java.util.List;
import java.util.Objects;

public class Wrapper<T> {
    List<T> records;

    public List<T> getRecords() {
        return records;
    }

    public void setRecords(List<T> records) {
        this.records = records;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Wrapper<?> wrapper)) return false;
        return Objects.equals(records, wrapper.records);
    }

    @Override
    public int hashCode() {
        return Objects.hash(records);
    }
}
