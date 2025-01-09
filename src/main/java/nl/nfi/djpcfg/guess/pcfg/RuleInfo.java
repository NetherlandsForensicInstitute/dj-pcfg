package nl.nfi.djpcfg.guess.pcfg;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.UUID;

public record RuleInfo(UUID uuid, Path basePath, String version, Charset encoding) {

}
