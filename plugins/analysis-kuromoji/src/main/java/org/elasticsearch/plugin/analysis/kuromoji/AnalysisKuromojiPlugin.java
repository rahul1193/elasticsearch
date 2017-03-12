/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugin.analysis.kuromoji;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.analysis.*;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;

public class AnalysisKuromojiPlugin extends Plugin implements AnalysisPlugin {

    // ClientConfiguration clinit has some classloader problems
    // This is also done for S3RepositoryPlugin, Ec2DiscoveryPlugin
    static {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                try {
                    Class.forName("com.amazonaws.ClientConfiguration");
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
                return null;
            }
        });
    }

    @Override
    public Map<String, AnalysisProvider<CharFilterFactory>> getCharFilters() {
        return singletonMap("kuromoji_iteration_mark", KuromojiIterationMarkCharFilterFactory::new);
    }

    @Override
    public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        Map<String, AnalysisProvider<TokenFilterFactory>> extra = new HashMap<>();
        extra.put("kuromoji_baseform", KuromojiBaseFormFilterFactory::new);
        extra.put("kuromoji_part_of_speech", KuromojiPartOfSpeechFilterFactory::new);
        extra.put("kuromoji_readingform", KuromojiReadingFormFilterFactory::new);
        extra.put("kuromoji_stemmer", KuromojiKatakanaStemmerFactory::new);
        extra.put("ja_stop", JapaneseStopTokenFilterFactory::new);
        extra.put("kuromoji_number", KuromojiNumberFilterFactory::new);
        return extra;
    }

    @Override
    public Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
        return singletonMap("kuromoji_tokenizer", KuromojiTokenizerFactory::new);
    }

    @Override
    public Map<String, AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> getAnalyzers() {
        return singletonMap("kuromoji", KuromojiAnalyzerProvider::new);
    }

    @Override
    public List<Setting<?>> getSettings() {
        // Plugin settings need to be registered from ES-5, otherwise Elasticsearch will reject them.
        return Arrays.asList(KuromojiDictionarySyncRunnable.AMAZON_S3_KEY_SETTING,
            KuromojiDictionarySyncRunnable.AMAZON_S3_SECRET_SETTING,
            KuromojiDictionarySyncRunnable.AMAZON_S3_BUCKET_SETTING,
            KuromojiDictionarySyncRunnable.AMAZON_S3_OBJECT_SETTING);
    }
}
