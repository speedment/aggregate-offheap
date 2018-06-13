package com.speedment.aggregate;

import com.speedment.enterprise.aggregator.Aggregation;
import com.speedment.enterprise.aggregator.Aggregator;
import com.speedment.runtime.compute.ToEnum;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;


public class AggregateBenchmark {

    // public static final int SIZE = 1_000_000;
    public static final int SIZE = 10_000_000;

    private List<Employee> employees;
    private Aggregator<Employee, ?, Result> aggregator1;
    private Aggregator<Employee, ?, Result> aggregator2;


    public void setup() {
        employees = Employee.generate()
            .limit(SIZE)
            .collect(Collectors.toList());

        aggregator1 = Aggregator.builderOfType(Employee.class, Result::new)

            //.on(ToEnum.of(Employee.Gender.class, Employee::gender)).key(Result::setGender)
            //.on((ToEnum<Employee, Employee.Gender>)Employee::gender).key(Result::setGender)
            //.on(ToInt.of(Employee::salary).divide(1000).asInt()).key(Result::setBracket)

            .on((Employee e) -> e.gender().ordinal()).key((Result r, int i) -> r.setGender(Employee.Gender.values()[i]))
            .on(Employee::salary).key(Result::setBracket)
            .count(Result::setCount)
            .build();

        aggregator2 = Aggregator.builderOfType(Employee.class, Result::new)
            //.on(ToEnum.of(Employee.Gender.class, Employee::gender)).key(Result::setGender)
            .on(Employee::hired).key(Result::setBracket)
            .on(Employee::salary).key(Result::setBracket)
            .count(Result::setCount)
            .build();
    }

    public void tearDown() {
        employees = null;
        aggregator1 = null;
    }

    //@Benchmark
    public Map<Employee.Gender, Map<Integer, Long>> aggregateJava2() {
        return employees.stream()
            .collect(
                groupingBy(Employee::gender,
                    //groupingBy((Employee emp) -> emp.salary() / 1000,
                    groupingBy(Employee::salary,
                        counting()
                    )
                )
            );
    }

    public Map<Integer, Map<Integer, Long>> aggregateJava1() {
        return employees.stream()
            .collect(
                groupingBy(Employee::hired,
                    groupingBy(Employee::salary,
                        counting()
                    )
                )

            );
    }

    public Aggregation<Result> aggregateOffHeap() {
        return employees.stream()
            .collect(
                aggregator2.createCollector()
            );
    }

    public Aggregation<Result> aggregateOffHeapAndClose() {
        final Aggregation<Result> result = aggregateOffHeap();
        result.close();
        return result;
    }


    private static final class Result {
        private Employee.Gender gender;
        private int bracket;
        private long count;

        public Employee.Gender getGender() {
            return gender;
        }

        public void setGender(Employee.Gender gender) {
            this.gender = gender;
        }

        public int getBracket() {
            return bracket;
        }

        public void setBracket(int bracket) {
            this.bracket = bracket;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }


    public static void main(String[] args) {
        final int samples = 10;
        final int parallelism = 4;

        final AggregateBenchmark ab = new AggregateBenchmark();
        System.out.println("setup");
        ab.setup();


        System.out.println("Running...");

        ab.aggregateOffHeap();

/*
        System.out.println("Start aggregation");
        try (Aggregation<Result> aggregation = ab.aggregateOffHeap()) {
            System.out.println("Counting");
            System.out.println(aggregation.stream().count());
        }

        System.out.println("Warmup phase");

        final Histogram hw1 = new Histogram("OffHeap",10, 2000);
        hw1.benchmark(ab::aggregateOffHeapAndClose, samples/10);
        System.out.println("Warmup 1 done");

        final Histogram hw2 = new Histogram("Heap",10, 2000);
        hw2.benchmark(ab::aggregateJava1, samples/10);
        System.out.println("Warmup 2 done");


        final Histogram h1 = new Histogram("OffHeap",10, 2000);
        h1.benchmark(ab::aggregateOffHeapAndClose, samples, parallelism);
        System.out.println(h1);

        final Histogram h2 = new Histogram("Heap",10, 2000);
        h2.benchmark(ab::aggregateJava1, samples, parallelism);
        System.out.println(h2); */

        System.out.println("tearDown");
        ab.tearDown();
    }



}
