package org.apache.lucene.analysis.ja;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ja.tokenattributes.PartOfSpeechAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 * @author rahulanishetty
 * @since 27/10/17.
 */
public class SprJapanesePartOfSpeechStopFilter extends TokenFilter {

    // # Remove whitespace tokens (part of speech is defined in stoptags.txt - "symbol-space : 記号-空白")
    private static final String WHITE_SPACE = "記号-空白";

    private final Set<String> stopTags;
    private final PartOfSpeechAttribute posAtt = addAttribute(PartOfSpeechAttribute.class);
    private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    private int skippedPositions;

    public SprJapanesePartOfSpeechStopFilter(TokenStream input) {
        this(input, Collections.emptySet());
    }

    public SprJapanesePartOfSpeechStopFilter(TokenStream input, Set<String> stopTags) {
        super(input);
        this.stopTags = stopTags;
    }


    @Override
    public final boolean incrementToken() throws IOException {
        skippedPositions = 0;
        while (input.incrementToken()) {
            if (isWhiteSpace()) {
                // donot increment position for white space
                continue;
            }
            if (accept()) {
                if (skippedPositions != 0) {
                    posIncrAtt.setPositionIncrement(posIncrAtt.getPositionIncrement() + skippedPositions);
                }
                return true;
            }
            skippedPositions += posIncrAtt.getPositionIncrement();
        }
        // reached EOS -- return false
        return false;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        skippedPositions = 0;
    }

    @Override
    public void end() throws IOException {
        super.end();
        posIncrAtt.setPositionIncrement(posIncrAtt.getPositionIncrement() + skippedPositions);
    }

    private boolean isWhiteSpace() {
        String pos = this.posAtt.getPartOfSpeech();
        return WHITE_SPACE.equals(pos);
    }

    private boolean accept() {
        String pos = this.posAtt.getPartOfSpeech();
        return pos == null || !this.stopTags.contains(pos);
    }
}
