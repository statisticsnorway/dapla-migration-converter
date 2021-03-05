package no.ssb.rawdata.converter.app.migration.csv;

import java.util.Set;

// TODO: Move to core

public enum DataType {
    STRING, BOOLEAN, INT("INTEGER"), LONG, DOUBLE;

    private final Set<String> aliases;

    DataType(String... aliases) {
        this.aliases = Set.of(aliases);
    }

    DataType() {
        aliases = Set.of();
    }

    public static DataType from(String s) {
        if (s == null) {
            return null;
        }

        for (DataType dataType : values()) {
            if (dataType.name().equalsIgnoreCase(s) || dataType.aliases.contains(s.toUpperCase())) {
                return dataType;
            }
        }

        throw new IllegalArgumentException("No DataType found matching name '" + s + "'");
    }

}
