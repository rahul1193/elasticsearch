package org.elasticsearch.index.analysis;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.analysis.ja.dict.UserDictionary;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class KuromojiUserDictionarySyncService extends AbstractComponent {

    public static final Setting<String> AMAZON_S3_KEY_SETTING = Setting.simpleString("kuromoji.dict.s3.key", Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<String> AMAZON_S3_SECRET_SETTING = Setting.simpleString("kuromoji.dict.s3.secret", Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<String> AMAZON_S3_BUCKET_SETTING = Setting.simpleString("kuromoji.dict.s3.bucket", Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<String> AMAZON_S3_OBJECT_SETTING = Setting.simpleString("kuromoji.dict.s3.object", Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<Boolean> AMAZON_S3_INIT_ON_START_SETTING = Setting.boolSetting("kuromoji.dict.s3.init.on.start", true, Setting.Property.Dynamic, Setting.Property.NodeScope);


    private AtomicLong lastSyncTime = new AtomicLong(-1);
    private final Set<Consumer<UserDictionary>> dictionaryConsumers = Collections.synchronizedSet(new HashSet<>());
    private final AtomicReference<UserDictionary> userDictionary = new AtomicReference<>();
    private final UserDictionaryConfig userDictionaryConfig;


    public KuromojiUserDictionarySyncService(ClusterService clusterService, ThreadPool threadPool) {
        super(clusterService.getSettings());

        userDictionaryConfig = new UserDictionaryConfig();
        userDictionaryConfig.setS3Key(AMAZON_S3_KEY_SETTING.get(clusterService.getSettings()));
        userDictionaryConfig.setS3Secret(AMAZON_S3_SECRET_SETTING.get(clusterService.getSettings()));
        userDictionaryConfig.setBucket(AMAZON_S3_BUCKET_SETTING.get(clusterService.getSettings()));
        userDictionaryConfig.setObjectPath(AMAZON_S3_OBJECT_SETTING.get(clusterService.getSettings()));

        clusterService.getClusterSettings().addSettingsUpdateConsumer(AMAZON_S3_KEY_SETTING, userDictionaryConfig::setS3Key);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AMAZON_S3_SECRET_SETTING, userDictionaryConfig::setS3Secret);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AMAZON_S3_BUCKET_SETTING, userDictionaryConfig::setBucket);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AMAZON_S3_OBJECT_SETTING, userDictionaryConfig::setObjectPath);

        if (AMAZON_S3_INIT_ON_START_SETTING.get(settings)) {
            initDictionaryAndLastSyncTime();
        }
        threadPool.schedule(TimeValue.timeValueSeconds(1), ThreadPool.Names.MANAGEMENT, new KurumojiSyncRunnable(threadPool));
    }

    private void initDictionaryAndLastSyncTime() {
        AmazonS3Client amazonS3Client = createAmazonS3Client();
        if (amazonS3Client == null) {
            return;
        }
        updateDictionaryAndLastSyncTime(amazonS3Client);
    }

    public void registerDictionaryConsumer(Consumer<UserDictionary> consumer) {
        dictionaryConsumers.add(consumer);
        if (lastSyncTime.get() != -1) {
            consumer.accept(userDictionary.get());
        }
    }

    private Tuple<UserDictionary, Long> downloadDictionary(AmazonS3Client amazonS3Client) throws IOException {
        String bucketKey = getBucketKey();
        String objectPath = getObjectPath();
        if (!Strings.hasLength(bucketKey) || !Strings.hasLength(objectPath)) {
            logger.warn("no bucketKey/object configuration found");
            return null;
        }
        GetObjectRequest request = new GetObjectRequest(bucketKey, objectPath);
        try (S3Object s3Object = amazonS3Client.getObject(request);
             InputStreamReader reader = new InputStreamReader(new BufferedInputStream(s3Object.getObjectContent()), Charset.defaultCharset())) {
            UserDictionary userDictionary = UserDictionary.open(reader);
            return Tuple.tuple(userDictionary, s3Object.getObjectMetadata().getLastModified().getTime());
        }
    }

    private AmazonS3Client createAmazonS3Client() {
        String s3Key = userDictionaryConfig.getS3Key();
        String s3Secret = userDictionaryConfig.getS3Secret();

        if (!Strings.hasLength(s3Key) || !Strings.hasLength(s3Secret)) {
            return null;
        }

        final SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        return AccessController.doPrivileged((PrivilegedAction<AmazonS3Client>) () -> new AmazonS3Client(new BasicAWSCredentials(s3Key, s3Secret), new ClientConfiguration().withMaxConnections(1)));
    }

    private long getSourceLastUpdateTime(AmazonS3Client amazonS3Client) {
        String bucketKey = getBucketKey();
        String objectPath = getObjectPath();
        if (!Strings.hasLength(bucketKey) || !Strings.hasLength(objectPath)) {
            logger.warn("no bucketKey/object configuration found");
            return 0;
        }
        ObjectMetadata metadata = amazonS3Client.getObjectMetadata(bucketKey, objectPath);
        if (metadata != null && metadata.getLastModified() != null) {
            return metadata.getLastModified().getTime();
        }
        return 0;
    }

    private String getObjectPath() {
        return userDictionaryConfig.getObjectPath();
    }

    private String getBucketKey() {
        return userDictionaryConfig.getBucket();
    }

    class KurumojiSyncRunnable extends AbstractRunnable implements ThreadPool.Cancellable {

        private final ThreadPool threadPool;
        private AtomicBoolean cancelled = new AtomicBoolean(false);

        KurumojiSyncRunnable(ThreadPool threadPool) {
            this.threadPool = threadPool;
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to run scheduled task [{}] on thread pool [{}]", this.toString(), threadPool.executor(ThreadPool.Names.SAME)), e);
        }

        @Override
        public void cancel() {
            cancelled.set(true);
        }

        @Override
        public boolean isCancelled() {
            return cancelled.get();
        }

        @Override
        public void onAfter() {
            if (lastSyncTime.get() == -1) {
                threadPool.schedule(TimeValue.timeValueSeconds(1), ThreadPool.Names.SAME, this);
            } else {
                threadPool.schedule(TimeValue.timeValueMinutes(1), ThreadPool.Names.SAME, this);
            }
        }

        @Override
        public void doRun() {
            if (settings == null) {
                return;
            }
            AmazonS3Client amazonS3Client = createAmazonS3Client();
            if (amazonS3Client == null) {
                logger.warn("no client configuration provided");
            }
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("Kuromoji dictionary sync triggered");
                }
                long s3UpdateTime = getSourceLastUpdateTime(amazonS3Client);
                if (s3UpdateTime > lastSyncTime.get()) {
                    UserDictionary userDictionary = updateDictionaryAndLastSyncTime(amazonS3Client);
                    for (Consumer<UserDictionary> consumer : dictionaryConsumers) {
                        try {
                            consumer.accept(userDictionary);
                        } catch (Exception e) {
                            logger.error("exception while syncing dictionary to consumer : {}" + consumer);
                        }
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("Kuromoji dictionary successfully updated for time " + lastSyncTime);
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Kuromoji dictionary left untouched");
                    }
                }
            } catch (Throwable t) {
                logger.error("An exception occured during kuromoji dictionary refresh : " + t.getMessage(), t);
            }
        }
    }

    private UserDictionary updateDictionaryAndLastSyncTime(AmazonS3Client amazonS3Client) {
        try {
            Tuple<UserDictionary, Long> userDictionaryModifiedTimeTuple = downloadDictionary(amazonS3Client);
            if (userDictionaryModifiedTimeTuple != null) {
                if (userDictionaryModifiedTimeTuple.v1() != null) {
                    userDictionary.set(userDictionaryModifiedTimeTuple.v1());
                    logger.info("synced kuromoji dictionary");
                }
                if (userDictionaryModifiedTimeTuple.v2() != null) {
                    lastSyncTime.set(userDictionaryModifiedTimeTuple.v2());
                }
                return userDictionaryModifiedTimeTuple.v1();
            }
        } catch (Throwable t) {
            logger.error("exception while downloading dictionary", t);
        }
        return null;
    }

    private static class UserDictionaryConfig {
        private String s3Key;
        private String s3Secret;
        private String bucket;
        private String objectPath;

        String getS3Key() {
            return s3Key;
        }

        void setS3Key(String s3Key) {
            this.s3Key = s3Key;
        }

        String getS3Secret() {
            return s3Secret;
        }

        void setS3Secret(String s3Secret) {
            this.s3Secret = s3Secret;
        }

        String getBucket() {
            return bucket;
        }

        void setBucket(String bucket) {
            this.bucket = bucket;
        }

        String getObjectPath() {
            return objectPath;
        }

        void setObjectPath(String objectPath) {
            this.objectPath = objectPath;
        }
    }
}
