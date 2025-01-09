package nl.nfi.djpcfg.guess.cache;

import nl.nfi.djpcfg.guess.pcfg.ParseTree;

import java.util.PriorityQueue;

public record Checkpoint(PriorityQueue<ParseTree> queue, ParseTree next, long keyspacePosition) {

}
