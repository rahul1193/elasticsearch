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
        JapaneseTokenizer tokenizer = new JapaneseTokenizer(userDict, false, mode);
        JapanesePartOfSpeechStopFilter stream = new JapanesePartOfSpeechStopFilter(tokenizer, stoptags);
        CJKWidthFilter stream2 = new CJKWidthFilter(stream);
        StopFilter stream3 = new StopFilter(stream2, stopwords);
        LowerCaseFilter stream1 = new LowerCaseFilter(stream3);
        return new TokenStreamComponents(tokenizer, stream1);
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
