package nl.nfi.djpcfg.guess.pcfg.grammar;

import java.util.List;

// subset of terminals, all entries with same probability
// e.g. in A4:
//      0.4    test
//      0.4    next
//      0.2    time
//  2 terminal groups, (0.4, [text, next]) and (0.2, [time])
public record TerminalGroup(double probability, List<String> values) {

}
