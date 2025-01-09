package nl.nfi.djpcfg.common.ini;

import java.util.List;

public final class IniSection {

    private final IniConfig iniConfig;
    private final String section;

    private IniSection(final IniConfig iniConfig, final String section) {
        this.iniConfig = iniConfig;
        this.section = section;
    }

    static IniSection ofConfig(final IniConfig iniConfig, final String section) {
        return new IniSection(iniConfig, section);
    }

    public String getString(final String key) {
        return iniConfig.getString(section, key);
    }

    public <T> List<T> getList(final String key) {
        return iniConfig.getList(section, key);
    }
}
