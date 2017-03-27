package org.elasticsearch.index.analysis;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.ja.SprJapaneseAnalyzer;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.TimeUnit;

/**
 * Created by sumanth on 08/03/17.
 */
public class KuromojiUserDictionarySyncUtil {

    private static final Logger logger = Loggers.getLogger(KuromojiUserDictionarySyncUtil.class);

    public static final String THREAD_NAME = "Sprinklr Kuromoji User Dictionary Sync Thread";


    public static String AMAZON_S3_KEY;
    public static String AMAZON_S3_SECRET;
    public static String AMAZON_S3_BUCKET;
    public static String AMAZON_S3_OBJECT;

    public static final String DICTIONARY_FILE = "/mnt1/tmp/kuromoji/dictionary.dat";
    public static final long REFRESH_INTERVAL = TimeUnit.MINUTES.toMillis(30);

    private static volatile KuromojiDictionarySyncRunnable syncRunnable;

    public static void ensureSyncThread(SprJapaneseAnalyzer analyzer, Settings settings) {
        if (syncRunnable == null) {
            synchronized (KuromojiUserDictionarySyncUtil.class) {
                if (syncRunnable == null) {
                    try {
                        KuromojiDictionarySyncRunnable runnable = new KuromojiDictionarySyncRunnable(analyzer, settings);
                        Thread syncThread = new Thread(runnable, THREAD_NAME);
                        syncThread.setDaemon(true);
                        syncThread.start();
                        syncRunnable = runnable;
                        syncRunnable.addAnalyzer(analyzer);
                    } catch (Throwable t) {
                        logger.error("Failed to start " + THREAD_NAME, t);
                    }
                } else {
                    syncRunnable.addAnalyzer(analyzer);
                }
            }
        } else {
            syncRunnable.addAnalyzer(analyzer);
        }
    }
}
