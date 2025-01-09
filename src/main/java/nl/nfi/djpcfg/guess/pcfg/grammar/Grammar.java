package nl.nfi.djpcfg.guess.pcfg.grammar;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class Grammar {

    private final Map<String, List<TerminalGroup>> sections;

    private Grammar() {
        this.sections = new LinkedHashMap<>();
    }

    public static Grammar empty() {
        return new Grammar();
    }

    // a section is a type of terminal of certain length (a variable), e.g.
    // A4, A5, D2, etc...
    public void addSection(final String name, final List<TerminalGroup> terminalGroups) {
        sections.put(name, terminalGroups);
    }

    public Set<String> sections() {
        return sections.keySet();
    }

    // name is e.g. A5
    public List<TerminalGroup> getSection(final String name) {
        final List<TerminalGroup> terminalGroups = sections.get(name);
        if (terminalGroups == null) {
            throw new IllegalStateException("No section found for " + name);
        }
        return terminalGroups;
    }
}
