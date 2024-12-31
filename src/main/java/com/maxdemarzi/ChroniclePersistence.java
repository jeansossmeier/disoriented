package com.maxdemarzi;

import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.index.Index;
import com.googlecode.cqengine.index.support.CloseableIterator;
import com.googlecode.cqengine.persistence.Persistence;
import com.googlecode.cqengine.persistence.support.ObjectStore;
import com.googlecode.cqengine.query.option.QueryOptions;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.apache.fury.Fury;
import org.apache.fury.config.CompatibleMode;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

public class ChroniclePersistence<O, A extends Comparable<A>> implements ObjectStore<O>, Persistence<O, A> {

    private final File dbFile;
    final SimpleAttribute<O, A> primaryKeyAttribute;
    final Fury fury;
    final Class<A> indexClass;
    final Class<O> objectClass;

    int objectMaxSize;
    int indexMaxSize;

    private ChronicleMap<A, O> chronicleMap;


    /**
     * Create's a chronicle persistence object, this will save the state to disk with a filesize proportional to the settings
     *
     * Note that the maximums below presume the worst case, (eg if your actual object size average is lower than what is set then you will be able to store more entries than indicated)
     * @param primaryKeyAttribute The primary attribute
     * @param dbFile The file to store in
     * @param indexClass The class of the indexing object
     * @param objectClass The class of the stored object
     * @param indexMaxSize The maximum expected size of an indexing object
     * @param objectMaxSize The maximum expected size of a stored object
     * @param maxEntries The maximum number of entries expected for this store
     * @throws IOException
     */
    public ChroniclePersistence(
            SimpleAttribute<O, A> primaryKeyAttribute,
            File dbFile,
            Class<A> indexClass,
            Class<O> objectClass,
            int indexMaxSize,
            int objectMaxSize,
            long maxEntries
    ) throws IOException {

        this.indexClass = indexClass;
        this.objectClass = objectClass;
        this.indexMaxSize  = indexMaxSize;
        this.objectMaxSize = objectMaxSize;

        this.fury = Fury.builder()
                .requireClassRegistration(true)
                .withCompatibleMode(CompatibleMode.SCHEMA_CONSISTENT)
                .build();

        this.fury.register(indexClass);
        this.fury.register(objectClass);

        this.primaryKeyAttribute = primaryKeyAttribute;
        this.dbFile = dbFile;

        ChronicleMapBuilder<A, O> mapBuilder = ChronicleMapBuilder.of(indexClass, objectClass)
                .name(dbFile.getName())
                .averageValueSize(objectMaxSize)
                .entries(maxEntries); // Adjust the expected number of entries as needed

        if (!Number.class.isAssignableFrom(indexClass)) {
            mapBuilder.averageKeySize(indexMaxSize);
        }

        try {
            chronicleMap = mapBuilder.createPersistedTo(dbFile);
        } catch (Exception e) {
            chronicleMap = mapBuilder.createOrRecoverPersistedTo(dbFile);
        }
    }


    @Override
    public ObjectStore<O> createObjectStore() {
        return this;
    }

    @Override
    public boolean supportsIndex(Index<O> index) {
        return true;
    }

    @Override
    public void openRequestScopeResources(QueryOptions queryOptions) {
        // No resources need to be opened for this implementation
    }

    @Override
    public void closeRequestScopeResources(QueryOptions queryOptions) {
        // No resources need to be closed for this implementation
    }

    @Override
    public SimpleAttribute<O, A> getPrimaryKeyAttribute() {
        return primaryKeyAttribute;
    }

    @Override
    public int size(QueryOptions queryOptions) {
        return (int) chronicleMap.longSize();
    }

    @Override
    public boolean contains(Object o, QueryOptions queryOptions) {
        A key = primaryKeyAttribute.getValue((O) o, queryOptions);
        return chronicleMap.containsKey(key);
    }

    @Override
    public boolean add(O o, QueryOptions queryOptions) {
        A key           = primaryKeyAttribute.getValue(o, queryOptions);
        O existingValue = chronicleMap.putIfAbsent(key, o);
        return existingValue == null;
    }

    @Override
    public boolean remove(Object o, QueryOptions queryOptions) {
        A key          = primaryKeyAttribute.getValue((O) o, queryOptions);
        O removedValue = chronicleMap.remove(key);
        return removedValue != null;
    }

    @Override
    public void clear(QueryOptions queryOptions) {
        chronicleMap.clear();
    }

    @Override
    public CloseableIterator<O> iterator(QueryOptions queryOptions) {
        return new ChronicleMapIterator<>(chronicleMap);
    }

    @Override
    public boolean isEmpty(QueryOptions queryOptions) {
        return size(queryOptions) == 0;
    }

    @Override
    public boolean containsAll(Collection<?> collection, QueryOptions queryOptions) {
        for (Object o : collection) {
            if (!contains(o, queryOptions)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends O> collection, QueryOptions queryOptions) {
        boolean modified = false;
        for (O o : collection) {
            if (add(o, queryOptions)) {
                modified = true;
            }
        }
        return modified;
    }

    @Override
    public boolean retainAll(Collection<?> collection, QueryOptions queryOptions) {
        boolean modified = false;
        try (CloseableIterator<O> iterator = iterator(queryOptions)) {
            while (iterator.hasNext()) {
                O o = iterator.next();
                if (!collection.contains(o)) {
                    iterator.remove();
                    modified = true;
                }
            }
        }
        return modified;
    }

    @Override
    public boolean removeAll(Collection<?> collection, QueryOptions queryOptions) {
        boolean modified = false;
        for (Object o : collection) {
            if (remove(o, queryOptions)) {
                modified = true;
            }
        }
        return modified;
    }


    // Helper methods to convert keys and values to ByteBuffers
    private ByteBuffer serializeKey(A key) {
        byte[] bytes = fury.serializeJavaObject(key);
        return ByteBuffer.wrap(bytes);
    }

    private ByteBuffer serializeValue(O value) {
        byte[] bytes = fury.serializeJavaObject(value);
        return ByteBuffer.wrap(bytes);
    }

    private A deserializeKey(ByteBuffer keyBuffer) {
        return fury.deserializeJavaObject(keyBuffer.array(), indexClass);
    }

    private O deserializeValue(ByteBuffer valueBuffer) {
        return fury.deserializeJavaObject(valueBuffer.array(), objectClass);
    }

    public File getDbFile() {
        return dbFile;
    }
}
