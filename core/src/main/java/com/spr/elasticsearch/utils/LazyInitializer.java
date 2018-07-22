package com.spr.elasticsearch.utils;

/**
 * @author rahulanishetty
 * @since 28/07/18.
 */
public class LazyInitializer<Type, ParamType> {

    public interface Creator<T, P> {

        T create(P param);
    }

    private final Creator<Type, ParamType> creator;
    private volatile Type instance;

    public LazyInitializer(Creator<Type, ParamType> creator) {
        this.creator = creator;
    }

    //https://en.wikipedia.org/wiki/Double-checked_locking
    public Type getOrCreate(ParamType param) {
        Type localVar = this.instance;
        if (localVar == null) {
            synchronized (this) {
                localVar = this.instance;
                if (localVar == null) {
                    localVar = this.instance = creator.create(param);
                }
            }
        }
        return localVar;
    }

    public Type getIfExist() {
        return instance;
    }

    public synchronized void setValueToNull() {
        instance = null;
    }
}
