package nl.nfi.djpcfg.guess.pcfg.generate;

import nl.nfi.djpcfg.guess.pcfg.ParseTree;
import nl.nfi.djpcfg.guess.pcfg.ReplacementSet;
import nl.nfi.djpcfg.guess.pcfg.grammar.Grammar;

import java.io.PrintStream;
import java.util.List;
import java.util.Random;

public final class GuesserCommon {

    static String generateGuess(final Grammar grammar, final ReplacementSet replacements, final Random random) {
        return generateGuess(grammar, replacements, random, 0, new StringBuilder());
    }

    static long writeGuesses(final Grammar grammar, final ParseTree parseTree, long skip, final long limit, final PrintStream output) {
        final ProgressState progress = new ProgressState();
        progress.skip = skip;
        progress.limit = limit;

        return writeGuesses(grammar, parseTree, 0, new StringBuilder(), progress, output);
    }

    private static String generateGuess(final Grammar grammar, final ReplacementSet replacements, final Random random, final int variableIndex, final StringBuilder guess) {
        final String category = replacements.categoryAt(variableIndex);
        final String variable = replacements.variableAt(variableIndex);
        final int index = replacements.indexAt(variableIndex);

        final boolean shouldPrintGuess = variableIndex + 1 == replacements.size();

        if (category.equals("C")) {
            final List<String> masks = grammar.getSection(variable).get(index).values();
            final String mask = masks.get(random.nextInt(masks.size()));

            final int maskLength = mask.length();

            final int start = guess.length() - maskLength;
            // TODO: make sure this always works
            //       most strings with casing are in BMP, so 2 byte chars suffices?
            //       does code break with >= 3 byte chars? even when not taking note of casing?
            for (int j = 0; j < maskLength; j++) {
                if (mask.charAt(j) != 'L') {
                    guess.setCharAt(start + j, Character.toUpperCase(guess.charAt(start + j)));
                }
            }

            if (shouldPrintGuess) {
                return guess.toString();
            } else {
                return generateGuess(grammar, replacements, random, variableIndex + 1, guess);
            }
        } else {
            final List<String> terminals = grammar.getSection(variable).get(index).values();
            final String value = terminals.get(random.nextInt(terminals.size()));
            guess.append(value);

            if (shouldPrintGuess) {
                return guess.toString();
            } else {
                return generateGuess(grammar, replacements, random, variableIndex + 1, guess);
            }
        }
    }

    private static long writeGuesses(final Grammar grammar, final ParseTree parseTree, final int variableIndex, final StringBuilder guess, final ProgressState progress, final PrintStream output) {
        long guessCount = 0;

        final ReplacementSet replacements = parseTree.replacementSet();
        final String category = replacements.categoryAt(variableIndex);
        final String variable = replacements.variableAt(variableIndex);
        final int index = replacements.indexAt(variableIndex);

        final boolean shouldPrintGuess = variableIndex + 1 == replacements.size();

        if (category.equals("C")) {
            final List<String> masks = grammar.getSection(variable).get(index).values();
            final int maskLength = masks.getFirst().length();

            for (final String mask : masks) {
                if (shouldPrintGuess && progress.skip > 0) {
                    progress.skip -= 1;
                    continue;
                }

                final int start = guess.length() - maskLength;
                // TODO: make sure this always works
                //       most strings with casing are in BMP, so 2 byte chars suffices?
                //       does code break with >= 3 byte chars? even when not taking note of casing?
                for (int j = 0; j < maskLength; j++) {
                    if (mask.charAt(j) != 'L') {
                        guess.setCharAt(start + j, Character.toUpperCase(guess.charAt(start + j)));
                    }
                }

                if (shouldPrintGuess) {
                    guessCount++;
                    progress.limit--;
                    output.println(guess.toString());
                } else {
                    guessCount += writeGuesses(grammar, parseTree, variableIndex + 1, guess, progress, output);
                }

                if (progress.limit <= 0) {
                    return guessCount;
                }
            }
        } else {
            for (final String value : grammar.getSection(variable).get(index).values()) {
                if (shouldPrintGuess && progress.skip > 0) {
                    progress.skip -= 1;
                    continue;
                }

                guess.append(value);

                if (shouldPrintGuess) {
                    guessCount++;
                    progress.limit--;
                    output.println(guess.toString());
                } else {
                    guessCount += writeGuesses(grammar, parseTree, variableIndex + 1, guess, progress, output);
                }

                guess.delete(guess.length() - value.length(), guess.length());

                if (progress.limit <= 0) {
                    return guessCount;
                }
            }
        }
        return guessCount;
    }
}
