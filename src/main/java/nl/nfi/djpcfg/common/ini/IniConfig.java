package nl.nfi.djpcfg.common.ini;

import org.json.JSONArray;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.nio.file.Files.readAllLines;

public final class IniConfig {

    private final Map<String, Map<String, Object>> sections;

    private IniConfig(final Map<String, Map<String, Object>> sections) {
        this.sections = sections;
    }

    public boolean hasSection(final String section) {
        return sections.containsKey(section);
    }

    public boolean hasKey(final String section, final String key) {
        return sections.containsKey(section) && sections.get(section).containsKey(key);
    }

    public IniSection getSection(final String section) {
        if (!hasSection(section)) {
            throw new IllegalArgumentException("INI config does not contain given section: %s".formatted(section));
        }
        return IniSection.ofConfig(this, section);
    }

    public String getString(final String section, final String key) {
        if (!hasKey(section, key)) {
            throw new IllegalArgumentException("INI config does not contain key in given section: %s -> %s".formatted(section, key));
        }
        return (String) sections.get(section).get(key);
    }

    public boolean getBoolean(final String section, final String key) {
        return Boolean.parseBoolean(getString(section, key));
    }

    public long getLong(final String section, final String key) {
        return Long.parseLong(getString(section, key));
    }

    public UUID getUUID(final String section, final String key) {
        return UUID.fromString(getString(section, key));
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> getList(final String section, final String key) {
        // TODO: unsafe cast for now
        return (List<T>) new JSONArray(getString(section, key)).toList();
    }

    public static IniConfig loadFrom(final Path path) throws IOException {
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("INI config file path does not exist: %s".formatted(path));
        }

        final Map<String, Map<String, Object>> sections = new LinkedHashMap<>();

        String sectionTitle = null;
        Map<String, Object> section = new LinkedHashMap<>();
        for (final String line : readAllLines(path)) {
            if (line.startsWith("[")) {
                if (sectionTitle != null) {
                    sections.put(sectionTitle, section);
                    section = new LinkedHashMap<>();
                }
                sectionTitle = line.substring(0, line.length() - 1).substring(1);
            } else {
                final String[] parts = line.split(" = ");
                if (parts.length == 1) {
                    section.put(parts[0], "");
                } else {
                    section.put(parts[0], parts[1]);
                }
            }
        }
        sections.put(sectionTitle, section);
        return new IniConfig(sections);
    }
}
