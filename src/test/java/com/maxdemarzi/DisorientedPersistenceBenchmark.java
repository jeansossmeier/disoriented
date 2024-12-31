package com.maxdemarzi;

import groovy.util.logging.Slf4j;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class DisorientedPersistenceBenchmark {
    private static final Logger log = LoggerFactory.getLogger(DisorientedPersistenceBenchmark.class);

    private Disoriented db;
    private Random rand = new Random();

    @Param({"10000000"})
    private int maxNodes;

    @Param({"10000000"})
    private int maxRels;

    @Param({"1000"})
    private int userCount;

    @Param({"1"})
    private int personCount;

    @Param({"200"})
    private int itemCount;

    @Param({"100"})
    private int friendsCount;

    @Param({"1000"})
    private int likesCount;

    @Setup(Level.Invocation )
    public void prepare() throws IOException {
        log.info("Preparing persistence for {} max nodes", maxNodes);

        Disoriented.init();
        Disoriented.initPersistence(32, 256, maxNodes);

        db = Disoriented.getInstance();

        for (int item = 0; item < itemCount; item++) {
            HashMap<String, Object> properties = new HashMap<>();
            properties.put("id", item);
            properties.put("itemname", "itemname" + item );
            db.addNode("User", "item" + item, properties);
        }

        for (int person = 0; person < personCount; person++) {
            for (int like = 0; like < itemCount; like++) {
                db.addRelationship("LIKES", "person" + person, "item" + rand.nextInt(itemCount));
            }
        }
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(4)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureCreateEmptyNode() throws IOException {
        db.addNode("User", "user" + rand.nextInt(userCount));
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(4)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public int measureCreateEmptyNodes() throws IOException {
        int user;
        for (user = 0; user < userCount; user++) {
            db.addNode("User", "user" + user);
        }
        return user;
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(4)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public int measureCreateNodesWithProperties() throws IOException {
        int user;
        for (user = 0; user < userCount; user++) {
            HashMap<String, Object> properties = new HashMap<>();
            properties.put("username", "username" + rand.nextInt() );
            properties.put("age", + rand.nextInt(100) );
            properties.put("weight", rand.nextInt(300) );
            db.addNode("User",  String.valueOf(rand.nextInt()), properties);
        }
        return user;
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(4)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureCreateNodeWithProperties() throws IOException {
        HashMap<String, Object> properties = new HashMap<>();
        properties.put("username", "username" + rand.nextInt() );
        properties.put("age", + rand.nextInt(100) );
        properties.put("weight", rand.nextInt(300) );
        db.addNode("User",  String.valueOf(rand.nextInt()), properties);
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(1)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public int measureCreateEmptyNodesAndRelationships() throws IOException {
        int user;
        for (user = 0; user < userCount; user++) {
            db.addNode("User", "user" + user);
        }
        for (user = 0; user < userCount; user++) {
            for (int like = 0; like < friendsCount; like++) {
                db.addRelationship("FRIENDS", "user" + user, "user" + rand.nextInt(userCount));
            }
        }
        return user;
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(4)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public int measureTraverse() throws IOException {
        int person;
        for (person = 0; person < personCount; person++) {
            db.getOutgoingRelationshipNodeIds("LIKES", "person" + person);
        }
        return person;
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(4)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public int measureTraverseAndGetNodes() throws IOException {
        int person;
        for (person = 0; person < personCount; person++) {
            db.getOutgoingRelationshipNodes("LIKES", "person" + person);
        }
        return person;
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(4)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureSingleTraversal() throws IOException {
        db.getOutgoingRelationshipNodeIds("LIKES", "person" + rand.nextInt(personCount));
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(4)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureSingleTraversalAndGetNodes() throws IOException {
        db.getOutgoingRelationshipNodes("LIKES", "person" + rand.nextInt(personCount));
    }

}
