package com.speedment.aggregate;

import java.util.IntSummaryStatistics;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;

public final class Histogram {

    private final String name;
    private final int resolutionMs;
    private final AtomicInteger[] buckets;

    public Histogram(
        final String name,
        final int resolutionMs,
        final int maxMs
    ) {
        this.name = requireNonNull(name);
        this.resolutionMs = resolutionMs;
        final int size = maxMs / resolutionMs;
        this.buckets = IntStream.range(0, size)
            .mapToObj(i -> new AtomicInteger())
            .toArray(AtomicInteger[]::new);
    }

    public void benchmark(Runnable r) {
        final long start = System.nanoTime();
        r.run();
        final long stop = System.nanoTime();
        final int durationMs = Math.toIntExact((stop - start) / 1_000_000L);
        add(durationMs);
    }

    public void benchmark(Runnable r, int iterations, int parallelism) {
        IntStream.range(0, parallelism)
            .parallel()
            .forEach(t -> {
                for (int i = 0; i < iterations; i++) {
                    benchmark(r);
                }
            });
    }


    public void benchmark(Runnable r, int iterations) {
        for (int i = 0; i < iterations; i++) {
            benchmark(r);
        }
    }

    public void add(int durationMs) {
        final int index = durationMs / resolutionMs;
        buckets[Math.min(index, buckets.length - 1)].incrementAndGet();
    }

    public IntUnaryOperator histogramGetter() {
        return i -> buckets[i].get();
    }

    public int resolutionMs() {
        return resolutionMs;
    }

    public String name() { return name; }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(String.format("name %s%n", name));
        sb.append(String.format("resolutionMS %d%n", resolutionMs));

        long sum = 0;
        long count = 0;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (int i = 0; i < buckets.length; i++) {
            final int bucket = buckets[i].get();
            if (bucket > 0) {
                long ms = i * resolutionMs;
                count += bucket;
                sum += bucket * ms;
                min = Math.min(min, (int) ms);
                max = Math.max(max, (int) ms);
            }
        }

        sb.append(String.format("samples %d%n", count));
        sb.append(String.format("average[ms] %f%n", (double) sum / (double) count));
        sb.append(String.format("min[ms] %d%n", min));
        sb.append(String.format("max[ms] %d%n", max));
        sb.append(String.format("%n"));

        sb.append(String.format("ms, samples%n"));
        for (int i = 0; i < buckets.length; i++) {
            sb.append(String.format("%d, %d%n", i * resolutionMs, buckets[i].get()));
        }

        return sb.toString();
    }
}
