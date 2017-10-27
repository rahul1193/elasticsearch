package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ja.SprJapanesePartOfSpeechStopFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author rahulanishetty
 * @since 27/10/17.
 */
public class SprKuromojiPartOfSpeechFilterFactory  extends AbstractTokenFilterFactory {

    private final Set<String> stopTags = new HashSet<>();

    public SprKuromojiPartOfSpeechFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);
        List<String> wordList = Analysis.getWordList(env, settings, "stoptags");
        if (wordList != null) {
            stopTags.addAll(wordList);
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new SprJapanesePartOfSpeechStopFilter(tokenStream, stopTags);
    }

}
