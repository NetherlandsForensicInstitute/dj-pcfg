package nl.nfi.djpcfg.guess.pcfg;

import nl.nfi.djpcfg.guess.pcfg.grammar.BaseStructure;
import nl.nfi.djpcfg.guess.pcfg.grammar.Grammar;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

import static java.util.Comparator.comparing;

public final class PcfgQueue {

    private final Pcfg pcfg;
    private final PriorityQueue<ParseTree> queue;

    public PcfgQueue(final Pcfg pcfg) {
        this.pcfg = pcfg;
        this.queue = new HeapLimitingPTQueue(comparing(ParseTree::probability).reversed());
        initializeQueue();
    }

    public PcfgQueue(final Pcfg pcfg, final PriorityQueue<ParseTree> queue) {
        this.pcfg = pcfg;
        this.queue = queue;
    }

    public static PcfgQueue fromStart(final Pcfg pcfg) {
        return new PcfgQueue(pcfg);
    }

    public static PcfgQueue loadFromExistingState(final Pcfg pcfg, final PriorityQueue<ParseTree> queue) {
        return new PcfgQueue(pcfg, queue);
    }

    public PriorityQueue<ParseTree> queue() {
        return queue;
    }

    private void initializeQueue() {
        for (final BaseStructure baseStructure : pcfg.baseStructures()) {
            final ReplacementSet replacements = new ReplacementSet(baseStructure.variables(), new int[baseStructure.variables().size()]);
            final ParseTree parseTree = new ParseTree(
                    baseStructure.probability(),
                    baseStructure.probability() * replacements.probability(pcfg.grammar()),
                    replacements
            );

            queue.add(parseTree);
        }
    }

    public boolean hasNext() {
        return !queue.isEmpty();
    }

    public ParseTree next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final ParseTree next = queue.poll();
        queue.addAll(findChildren(pcfg.grammar(), next));
        return next;
    }

    public List<ParseTree> findChildren(final Grammar grammar, final ParseTree parent) {
        final List<ParseTree> children = new ArrayList<>();

        final double parentProbability = parent.probability();
        final ReplacementSet parentReplacements = parent.replacementSet();

        for (int position = 0; position < parentReplacements.size(); position++) {
            final String variable = parentReplacements.variableAt(position);
            final int index = parentReplacements.indexAt(position);

            if (grammar.getSection(variable).size() == index + 1) {
                continue;
            }

            final ReplacementSet childReplacements = parentReplacements.withIndexIncrementedAt(position);
            
            if (areYouMyChild(grammar, childReplacements, parent.baseProbability(), position, parentProbability)) {
                children.add(new ParseTree(
                        parent.baseProbability(),
                        parent.baseProbability() * childReplacements.probability(pcfg.grammar()),
                        childReplacements
                ));
            }
        }

        return children;
    }

    private boolean areYouMyChild(final Grammar grammar, final ReplacementSet childReplacements, final double baseProbability, final int parentPosition, final double parentProbability) {
        // TODO: could precalculate probabilities for each index and increment index?
        for (int position = 0; position < childReplacements.size(); position++) {
            if (position == parentPosition) {
                continue;
            }
            if (childReplacements.indexAt(position) == 0) {
                continue;
            }

            double newParentProbability = baseProbability;
            for (int i = 0; i < childReplacements.size(); i++) {
                int index = childReplacements.indexAt(i);
                if (i == position) {
                    index--;
                }
                newParentProbability *= grammar.getSection(childReplacements.variableAt(i)).get(index).probability();
            }

            if (newParentProbability < parentProbability) {
                return false;
            }
            if (newParentProbability == parentProbability) {
                if (position < parentPosition) {
                    return false;
                }
            }
        }
        return true;
    }
}