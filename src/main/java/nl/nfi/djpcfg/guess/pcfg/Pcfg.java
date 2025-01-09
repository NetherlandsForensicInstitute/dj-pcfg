package nl.nfi.djpcfg.guess.pcfg;

import nl.nfi.djpcfg.common.ini.IniConfig;
import nl.nfi.djpcfg.common.ini.IniSection;
import nl.nfi.djpcfg.guess.pcfg.grammar.BaseStructure;
import nl.nfi.djpcfg.guess.pcfg.grammar.Grammar;
import nl.nfi.djpcfg.guess.pcfg.grammar.TerminalGroup;
import nl.nfi.djpcfg.serialize.PcfgCodec;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.nio.file.Files.exists;
import static java.nio.file.Files.lines;

public final class Pcfg {

    private static final String VERSION = "4.6.0";

    private final RuleInfo ruleInfo;
    private final Grammar grammar;
    private final List<BaseStructure> baseStructures;

    private Pcfg(final RuleInfo ruleInfo, final Grammar grammar, final List<BaseStructure> baseStructures) {
        this.ruleInfo = ruleInfo;
        this.grammar = grammar;
        this.baseStructures = baseStructures;
    }

    public RuleInfo ruleInfo() {
        return ruleInfo;
    }

    public Grammar grammar() {
        return grammar;
    }

    public List<BaseStructure> baseStructures() {
        return baseStructures;
    }

    public BigInteger keyspaceSize() {
        BigInteger totalSize = BigInteger.ZERO;

        for (final BaseStructure baseStructure : baseStructures) {
            BigInteger size = BigInteger.ONE;
            for (final String variable : baseStructure.variables()) {
                final long transitionCount = grammar.getSection(variable).stream()
                        .map(TerminalGroup::values)
                        .mapToLong(List::size)
                        .sum();
                size = size.multiply(BigInteger.valueOf(transitionCount));
            }
            totalSize = totalSize.add(size);
        }

        return totalSize;
    }

    public static Pcfg create(final RuleInfo ruleInfo, final Grammar grammar, final List<BaseStructure> baseStructures) {
        return new Pcfg(ruleInfo, grammar, baseStructures);
    }

    public static Pcfg loadFrom(final Path basePath) throws IOException {
        if (!exists(basePath)) {
            throw new IllegalArgumentException("PCFG model path does not exist: %s".formatted(basePath));
        }
        // if a PCFG binary model, load serialized form
        if (Files.isRegularFile(basePath)) {
            return PcfgCodec.forInput(new BufferedInputStream(new FileInputStream(basePath.toFile()))).read();
        }

        final IniConfig iniConfig = IniConfig.loadFrom(basePath.resolve("config.ini"));
        // TODO: check version vs Pcfg.VERSION
        final Charset encoding = Charset.forName(iniConfig.getString("TRAINING_DATASET_DETAILS", "encoding"));
        final RuleInfo info = new RuleInfo(
                iniConfig.getUUID("TRAINING_DATASET_DETAILS", "uuid"),
                basePath,
                iniConfig.getString("TRAINING_PROGRAM_DETAILS", "version"),
                encoding
        );

        final Grammar grammar = Grammar.empty();

        // load terminals
        loadTerminals(iniConfig.getSection("BASE_A"), basePath, encoding, grammar);
        loadTerminals(iniConfig.getSection("CAPITALIZATION"), basePath, encoding, grammar);
        loadTerminals(iniConfig.getSection("BASE_D"), basePath, encoding, grammar);
        loadTerminals(iniConfig.getSection("BASE_O"), basePath, encoding, grammar);
        loadTerminals(iniConfig.getSection("BASE_K"), basePath, encoding, grammar);
        loadTerminals(iniConfig.getSection("BASE_Y"), basePath, encoding, grammar);
        loadTerminals(iniConfig.getSection("BASE_X"), basePath, encoding, grammar);

        // custom terminals
        grammar.addSection("E", readTerminals(basePath.resolve("Emails", "email_providers.txt"), encoding));
        grammar.addSection("W", readTerminals(basePath.resolve("Websites", "website_hosts.txt"), encoding));

        final List<BaseStructure> baseStructures = readBaseStructures(basePath.resolve("Grammar", "grammar.txt"));
        return new Pcfg(info, grammar, baseStructures);
    }

    private static List<BaseStructure> readBaseStructures(final Path baseStructuresFilePath) throws IOException {
        // TODO: close stream?
        double totalProbability = 1.0;
        try (final Stream<String> lines = lines(baseStructuresFilePath)) {
            totalProbability -= lines
                    .map(line -> line.split("\t"))
                    .filter(parts -> parts[0].equals("M"))
                    .mapToDouble(parts -> Double.parseDouble(parts[1]))
                    .findAny()
                    .orElse(0.0);
        }

        final List<BaseStructure> baseStructures = new ArrayList<>();

        for (final String line : iter(lines(baseStructuresFilePath))) {
            final String[] parts = line.split("\t");
            final String value = parts[0];
            final double probability = Double.parseDouble(parts[1]) / totalProbability;

            // ignore Markov
            if (value.equals("M")) {
                continue;
            }

            final List<String> replacements = new ArrayList<>(List.of(value.split("(?<=\\d|\\D)(?=\\D)")));

            for (int i = 0; i < replacements.size(); i++) {
                if (replacements.get(i).startsWith("A")) {
                    final int length = Integer.parseInt(replacements.get(i).substring(1));
                    replacements.add(i + 1, "C" + length);
                }
            }

            baseStructures.add(new BaseStructure(probability, replacements));
        }
        return baseStructures;
    }

    private static void loadTerminals(final IniSection section, final Path basePath, final Charset encoding, final Grammar grammar) throws IOException {
        final Path directory = basePath.resolve(section.getString("directory"));
        final List<String> fileNames = section.getList("filenames");

        for (final String fileName : fileNames) {
            final Path terminalsFilePath = directory.resolve(fileName);
            final String name = section.getString("name") + fileName.split("\\.")[0];
            grammar.addSection(name, readTerminals(terminalsFilePath, encoding));
        }
    }

    private static List<TerminalGroup> readTerminals(final Path filePath, final Charset encoding) throws IOException {
        List<TerminalGroup> terminalGroups = new ArrayList<>();
        double previousProbability = -1.0;
        List<String> values = new ArrayList<>();

        for (final String line : iter(lines(filePath, encoding))) {
            final String[] parts = line.split("\t");
            final String value = parts[0];
            final double probability = Double.parseDouble(parts[1]);

            if (probability != previousProbability) {
                if (!values.isEmpty()) {
                    terminalGroups.add(new TerminalGroup(previousProbability, values));
                }
                values = new ArrayList<>();
                previousProbability = probability;
            }
            values.add(value);
        }
        terminalGroups.add(new TerminalGroup(previousProbability, values));

        return terminalGroups;
    }

    // TODO: close stream?
    public static <T> Iterable<T> iter(final Stream<T> stream) {
        return stream::iterator;
    }

    public long calculateTerminalCount(final Grammar grammar, final ParseTree next) {
        long terminalCount = 1;

        final ReplacementSet replacements = next.replacementSet();
        for (int position = 0; position < replacements.size(); position++) {
            terminalCount *= grammar.getSection(replacements.variableAt(position))
                    .get(replacements.indexAt(position))
                    .values()
                    .size();
        }

        return terminalCount;
    }
}
