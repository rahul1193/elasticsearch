package org.elasticsearch.index.analysis;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.commons.codec.Charsets;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.ja.SprJapaneseAnalyzer;
import org.apache.lucene.analysis.ja.dict.UserDictionary;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.analysis.KuromojiUserDictionarySyncUtil.*;

/**
 * @author Utkarsh
 */
public class KuromojiDictionarySyncRunnable implements Runnable {

    private static final Logger logger = Loggers.getLogger(KuromojiDictionarySyncRunnable.class);

    public static final Setting<String> AMAZON_S3_KEY_SETTING = Setting.simpleString("s3.key", Setting.Property.NodeScope);
    public static final Setting<String> AMAZON_S3_SECRET_SETTING = Setting.simpleString("s3.secret", Setting.Property.NodeScope);
    public static final Setting<String> AMAZON_S3_BUCKET_SETTING = Setting.simpleString("s3.bucket", Setting.Property.NodeScope);
    public static final Setting<String> AMAZON_S3_OBJECT_SETTING = Setting.simpleString("s3.object", Setting.Property.NodeScope);

    private final Set<SprJapaneseAnalyzer> analyzers = Collections.synchronizedSet(new HashSet<>());
    private final AmazonS3Client amazonS3Client;
    private long lastSyncTime;
    private volatile UserDictionary userDict;

    public KuromojiDictionarySyncRunnable(SprJapaneseAnalyzer analyzer, Settings settings) {
        initializeSettings(settings);
        AWSCredentials credentials = new BasicAWSCredentials(AMAZON_S3_KEY, AMAZON_S3_SECRET);
        this.amazonS3Client = new AmazonS3Client(credentials);
        this.analyzers.add(analyzer);
        this.lastSyncTime = 0;
    }

    public void addAnalyzer(SprJapaneseAnalyzer analyzer) {
        this.analyzers.add(analyzer);
        analyzer.setUserDictionary(userDict);
    }

    @Override
    public void run() {
        while (true) {
            try {
                try {
                    logger.info("Kuromoji dictionary sync triggered");

                    long s3UpdateTime = getLastS3UpdateTime();
                    if (s3UpdateTime > lastSyncTime) {
                        userDict = downloadDictionary();
                        for (SprJapaneseAnalyzer analyzer : analyzers) {
                            analyzer.setUserDictionary(userDict);
                        }
                        logger.info("Kuromoji dictionary successfully updated for time " + lastSyncTime);
                    } else {
                        logger.info("Kuromoji dictionary left untouched");
                    }

                } catch (Throwable t) {
                    logger.error("An exception occured during kuromoji dictionary refresh : " + t.getMessage(), t);
                }

                reallySleep(REFRESH_INTERVAL);
            } catch (Throwable t) {
                logger.error("Unexpected exception occured during kuromoji dictionary refresh : " + t.getMessage(), t);
            }
        }
    }

    private UserDictionary downloadDictionary() throws IOException {
        GetObjectRequest request = new GetObjectRequest(AMAZON_S3_BUCKET, AMAZON_S3_OBJECT);
        File dictionaryFile = null;
        UserDictionary dictionary = null;
        try {
            dictionaryFile = new File(DICTIONARY_FILE);
            ObjectMetadata metadata = amazonS3Client.getObject(request, dictionaryFile);
            dictionary = readUserDictionary(dictionaryFile);
            lastSyncTime = metadata.getLastModified().getTime();
        } finally {
            try {
                dictionaryFile.delete();
            } catch (Throwable t) {
                // It's okay. You tried.
            }
        }
        return dictionary;
    }

    private UserDictionary readUserDictionary(File file) throws IOException {
        try {
            InputStreamReader reader = new InputStreamReader(new FileInputStream(file), Charsets.UTF_8);
            return UserDictionary.open(reader);
        } catch (IOException var8) {
            String message = String.format(Locale.ROOT, "IOException while reading %s : %s", file.getAbsolutePath(), var8.getMessage());
            throw new IOException(message, var8);
        }
    }

    private long getLastS3UpdateTime() {
        ObjectMetadata metadata = amazonS3Client.getObjectMetadata(AMAZON_S3_BUCKET, AMAZON_S3_OBJECT);
        if (metadata != null && metadata.getLastModified() != null) {
            return metadata.getLastModified().getTime();
        }

        return 0;
    }

    private void reallySleep(long millis) {
        boolean threadInterrupted = false;
        final long nanos = TimeUnit.MILLISECONDS.toNanos(millis);
        final long end = System.nanoTime() + nanos;
        long remaining;
        try {
            do {
                remaining = end - System.nanoTime();
                if (remaining <= 0) {
                    break;
                }
                try {
                    Thread.sleep(TimeUnit.NANOSECONDS.toMillis(remaining));
                } catch (InterruptedException e) {
                    threadInterrupted = true;
                }
            } while (remaining > 0);
        } finally {
            if (threadInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void initializeSettings(Settings settings) {
        AMAZON_S3_KEY = getSettingValue(AMAZON_S3_KEY_SETTING, settings);
        AMAZON_S3_SECRET = getSettingValue(AMAZON_S3_SECRET_SETTING, settings);
        AMAZON_S3_BUCKET = getSettingValue(AMAZON_S3_BUCKET_SETTING, settings);
        AMAZON_S3_OBJECT = getSettingValue(AMAZON_S3_OBJECT_SETTING, settings);
    }

    private static String getSettingValue(Setting<String> setting, Settings settings) {
        if (!setting.exists(settings)) {
            throw new IllegalArgumentException(setting.getKey() + " not set in config file - elasticsearch.yml.");
        }
        return setting.get(settings);
    }
}
