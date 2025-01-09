package nl.nfi.djpcfg.guess.pcfg;

import nl.nfi.djpcfg.guess.pcfg.grammar.Grammar;

import java.util.List;

import static java.lang.String.join;
import static java.util.Arrays.copyOf;

// TODO: pass baste struct instead of variables?
// set of variables (i.e. a base struct, A4D2O1, variables = [A4, D2, O1])
// set of indices pointing to the next TerminalGroup in that variable section
public record ReplacementSet(List<String> variables, int[] indices) {

    public String variableAt(final int position) {
        return variables.get(position);
    }

    public int indexAt(final int position) {
        return indices[position];
    }

    public int size() {
        return variables.size();
    }

    public String categoryAt(final int position) {
        return variableAt(position).substring(0, 1);
    }

    public double probability(final Grammar grammar) {
        double probability = 1.0;
        for (int position = 0; position < size(); position++) {
            probability *= grammar.getSection(variableAt(position)).get(indexAt(position)).probability();

        }
        return probability;
    }

    public ReplacementSet withIndexIncrementedAt(final int position) {
        final int[] indices = copyOf(this.indices, this.indices.length);
        indices[position]++;
        return new ReplacementSet(variables, indices);
    }

    public String declaredStructure() {
        return join("", variables.stream().filter(v -> !v.startsWith("C")).toList());
    }

    public String actualStructure() {
        return join(" ", variables);
    }
}
