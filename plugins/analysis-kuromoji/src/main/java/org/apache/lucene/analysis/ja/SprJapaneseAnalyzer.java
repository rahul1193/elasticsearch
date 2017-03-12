package org.apache.lucene.analysis.ja;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.cjk.CJKWidthFilter;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.ja.dict.UserDictionary;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.KuromojiUserDictionarySyncUtil;

import java.util.Set;

/**
 * Created by sumanth on 08/03/17.
 */
public class SprJapaneseAnalyzer extends StopwordAnalyzerBase {
    private final JapaneseTokenizer.Mode mode;
    private final Set<String> stoptags;
    private volatile UserDictionary userDict;

    public SprJapaneseAnalyzer(UserDictionary userDict, JapaneseTokenizer.Mode mode, CharArraySet stopwords, Set<String> stoptags, Settings settings) {
        super(stopwords);
        this.userDict = userDict;
        this.mode = mode;
        this.stoptags = stoptags;
        KuromojiUserDictionarySyncUtil.ensureSyncThread(this, settings);
    }

    public void setUserDictionary(UserDictionary userDict) {
        this.userDict = userDict;
        getReuseStrategy().setReusableComponents(this, "ignored", null);
    }

    @Override
    protected Analyzer.TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new JapaneseTokenizer(userDict, false, mode);
        TokenStream stream = new JapanesePartOfSpeechStopFilter(tokenizer, stoptags);
        stream = new CJKWidthFilter(stream);
        stream = new StopFilter(stream, stopwords);
        stream = new LowerCaseFilter(stream);
        return new TokenStreamComponents(tokenizer, stream);
    }

    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
        CJKWidthFilter result = new CJKWidthFilter(in);
        org.apache.lucene.analysis.LowerCaseFilter result1 = new org.apache.lucene.analysis.LowerCaseFilter(result);
        return result1;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj;
    }
}
