package nl.nfi.djpcfg.guess.pcfg;

// probability of base structure
// probability of generated passwords of this parse tree
// base structure with indices to each terminal group for each variable in the base structure
public record ParseTree(double baseProbability, double probability, ReplacementSet replacementSet) {

}
