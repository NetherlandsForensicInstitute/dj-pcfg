package nl.nfi.djpcfg.guess.pcfg.grammar;

import java.util.List;

import static java.lang.String.join;

// a single base structure entry, e.g.:
//      0.2    A4D2O1
//  where variables = [A4, D2, O1]
public record BaseStructure(double probability, List<String> variables) {

    public String declaredStructure() {
        return join("", variables.stream().filter(v -> !v.startsWith("C")).toList());
    }
}
