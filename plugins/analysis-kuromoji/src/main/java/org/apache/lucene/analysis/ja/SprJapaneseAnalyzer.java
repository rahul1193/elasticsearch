package org.apache.lucene.analysis.ja;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.cjk.CJKWidthFilter;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.ja.dict.UserDictionary;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.analysis.KuromojiUserDictionarySyncService;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Created by sumanth on 08/03/17.
 */
public class SprJapaneseAnalyzer extends StopwordAnalyzerBase {
    private final JapaneseTokenizer.Mode mode;
    private final Set<String> stoptags;
    private volatile UserDictionary userDict;

    private final AtomicBoolean registered = new AtomicBoolean();
    private final AtomicBoolean userDictionaryInitialized = new AtomicBoolean();
    // # Remove whitespace tokens (part of speech is defined in stoptags.txt - "symbol-space : 記号-空白")
    private static final Set<String> DEFAULT_STOP_TAGS = Sets.newHashSet("記号-空白");
    private final Supplier<KuromojiUserDictionarySyncService> userDictionarySyncService;

    public SprJapaneseAnalyzer(UserDictionary userDict, JapaneseTokenizer.Mode mode, CharArraySet stopwords, Set<String> stoptags, Supplier<KuromojiUserDictionarySyncService> userDictionarySyncService) {
        super(stopwords);
        this.userDict = userDict;
        this.mode = mode;
        this.stoptags = stoptags;
        this.userDictionarySyncService = userDictionarySyncService;
    }

    @Override
    protected Analyzer.TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = getTokenizer();
        JapanesePartOfSpeechStopFilter stream = new JapanesePartOfSpeechStopFilter(tokenizer, stoptags);
        CJKWidthFilter stream2 = new CJKWidthFilter(stream);
        StopFilter stream3 = new StopFilter(stream2, stopwords);
        LowerCaseFilter stream1 = new LowerCaseFilter(stream3);
        return new TokenStreamComponents(tokenizer, stream1);
    }

    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
        CJKWidthFilter result = new CJKWidthFilter(in);
        return new org.apache.lucene.analysis.LowerCaseFilter(result);
    }

    public static Set<String> getDefaultStopTags() {
        return DEFAULT_STOP_TAGS;
    }

    /**
     * @return The base tokenizer without the additional filters, used in createComponents() method.
     */
    public Tokenizer getTokenizer() {
        if (registered.compareAndSet(false, true)) {
            userDictionarySyncService.get().registerDictionaryConsumer(userDictionary -> {
                setUserDictionary(userDictionary);
                userDictionaryInitialized.set(true);
                synchronized (userDictionaryInitialized) {
                    userDictionaryInitialized.notifyAll();
                }
            });
        }

        if (!userDictionaryInitialized.get()) {
            try {
                synchronized (userDictionaryInitialized) {
                    if (!userDictionaryInitialized.get()) {
                        userDictionaryInitialized.wait(5000L);
                    }
                }
            } catch (InterruptedException e) {
                //ignore
            }
        }

        return new JapaneseTokenizer(userDict, false, mode);
    }

    private void setUserDictionary(UserDictionary userDict) {
        this.userDict = userDict;
        getReuseStrategy().setReusableComponents(this, "ignored", null);
    }
}
