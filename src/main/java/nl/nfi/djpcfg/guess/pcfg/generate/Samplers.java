package nl.nfi.djpcfg.guess.pcfg.generate;

import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import nl.nfi.djpcfg.guess.pcfg.ReplacementSet;
import nl.nfi.djpcfg.guess.pcfg.grammar.BaseStructure;
import nl.nfi.djpcfg.guess.pcfg.grammar.TerminalGroup;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.function.Function;
import java.util.function.IntToDoubleFunction;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;

public final class Samplers {

    static ToIntFunction<Random> buildIndexSampler(final int n, final IntToDoubleFunction pf) {
        final double[] u = new double[n];
        final int[] k = new int[n];

        final Queue<Integer> small = new ArrayDeque<>();
        final Queue<Integer> large = new ArrayDeque<>();

        for (int element = 0; element < n; element++) {
            u[element] = pf.applyAsDouble(element) * n;

            if (u[element] < 1.0) {
                small.add(element);
            } else {
                large.add(element);
            }
        }

        while (!(small.isEmpty() || large.isEmpty())) {
            final int l = small.poll();
            final int g = large.poll();

            k[l] = g;

            final double pg = u[g] + u[l] - 1.0;
            u[g] = pg;

            if (u[g] < 1.0) {
                small.add(g);
            } else {
                large.add(g);
            }
        }

        while (!large.isEmpty()) {
            u[large.poll()] = 1.0;
        }
        while (!small.isEmpty()) {
            u[small.poll()] = 1.0;
        }

        return random -> {
            final int i = random.nextInt(n);
            if (random.nextDouble() <= u[i]) {
                return i;
            }
            return k[i];
        };
    }

    static <T> Function<Random, T> buildSampler(final List<T> elements, final ToDoubleFunction<T> pf) {
        final int n = elements.size();

        final Map<T, Integer> indices = new HashMap<>();
        for (int i = 0; i < n; i++) {
            indices.put(elements.get(i), i);
        }

        final double[] u = new double[n];
        @SuppressWarnings("unchecked") // is safe, we use the objects themselves
        final T[] k = (T[]) new Object[n];

        final Queue<T> small = new ArrayDeque<>();
        final Queue<T> large = new ArrayDeque<>();

        for (int i = 0; i < n; i++) {
            final T element = elements.get(i);

            u[i] = pf.applyAsDouble(element) * n;

            if (u[i] < 1.0) {
                small.add(element);
            } else {
                large.add(element);
            }
        }

        while (!(small.isEmpty() || large.isEmpty())) {
            final T l = small.poll();
            final T g = large.poll();

            final int lIdx = indices.get(l);
            final int gIdx = indices.get(g);

            k[lIdx] = g;

            final double pg = u[gIdx] + u[lIdx] - 1.0;
            u[gIdx] = pg;

            if (u[gIdx] < 1.0) {
                small.add(g);
            } else {
                large.add(g);
            }
        }

        while (!large.isEmpty()) {
            u[indices.get(large.poll())] = 1.0;
        }
        while (!small.isEmpty()) {
            u[indices.get(small.poll())] = 1.0;
        }

        return random -> {
            final int i = random.nextInt(n);
            if (random.nextDouble() <= u[i]) {
                return elements.get(i);
            }
            return k[i];
        };
    }

    static Function<Random, ReplacementSet> buildReplacementsSampler(final Pcfg pcfg) {
        final Function<Random, BaseStructure> baseSampler = buildSampler(pcfg.baseStructures(), BaseStructure::probability);
        final Map<String, ToIntFunction<Random>> terminalGroupSamplers = new HashMap<>(pcfg.grammar().sections().size());
        pcfg.grammar().sections().forEach(section -> {
            final List<TerminalGroup> groups = pcfg.grammar().getSection(section);
            terminalGroupSamplers.put(section, buildIndexSampler(groups.size(), index -> groups.get(index).probability()));
        });

        return random -> {
            final BaseStructure base = baseSampler.apply(random);
            final List<String> variables = base.variables();
            final int[] indices = new int[variables.size()];
            for (int i = 0; i < variables.size(); i++) {
                final String type = variables.get(i);
                indices[i] = terminalGroupSamplers.get(type).applyAsInt(random);
            }
            return new ReplacementSet(variables, indices);
        };
    }
}
