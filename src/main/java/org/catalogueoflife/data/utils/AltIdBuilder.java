package org.catalogueoflife.data.utils;

import life.catalogue.api.model.Identifier;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class AltIdBuilder {
    final List<Identifier> ids = new ArrayList<>();

    public void add(Identifier id) {
        ids.add(id);
    }
    public void add(Identifier.Scope scope, String id) {
        add(scope.prefix(), id);
    }
    public void add(String scope, String id) {
        if (!StringUtils.isBlank(id)) {
            if (id.contains(",")) throw new IllegalArgumentException("Identifier not allowed to contain commas");
            ids.add(new Identifier(scope, id));
        }
    }
    @Override
    public String toString() {
        if (ids.isEmpty()) return null;
        return ids.stream().map(id -> id.toString()).collect(Collectors.joining(","));
    }
}
