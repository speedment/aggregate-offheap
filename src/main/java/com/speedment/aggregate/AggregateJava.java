package com.speedment.aggregate;

import com.speedment.enterprise.aggregator.Aggregation;
import com.speedment.enterprise.aggregator.Aggregator;
import com.speedment.runtime.compute.ToEnum;
import com.speedment.runtime.compute.ToInt;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

@State(Scope.Benchmark)
public class AggregateJava {

    public static final int SIZE = 10_000_000;

    private List<Employee> employees;
    private Aggregator<Employee, ?, Result> aggregator1;
    private Aggregator<Employee, ?, Result> aggregator2;

    @Setup
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
            .on(ToEnum.of(Employee.Gender.class, Employee::gender)).key(Result::setGender)
            .on(Employee::salary).key(Result::setBracket)
            .count(Result::setCount)
            .build();
    }

    @TearDown
    public void tearDown() {
        employees = null;
        aggregator1 = null;
    }

    //@Benchmark
    public Map<Employee.Gender, Map<Integer, Long>> aggregateJava() {
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

    @Benchmark
    public Aggregation<Result> aggregateOffHeap() {
        return employees.stream()
            .collect(
                aggregator2.createCollector()
            );
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


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(AggregateJava.class.getSimpleName())
            .mode(Mode.AverageTime)
            .threads(Threads.MAX)
            .forks(1)
            .warmupIterations(5)
            .measurementIterations(5)
            .build();

        new Runner(opt).run();

    }



}
