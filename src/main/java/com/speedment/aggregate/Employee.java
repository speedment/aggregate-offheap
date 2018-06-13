package com.speedment.aggregate;

import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public final class Employee {

    private final int id;
    private final String name;
    private final Gender gender;
    private final int salary;
    private final int hired;
    private final int deptId;

    public Employee(
        final int id,
        final String name,
        final Gender gender,
        final int salary,
        final int hired,
        final int deptId
    ) {
        this.id = id;
        this.name = requireNonNull(name);
        this.gender = gender;
        this.salary = salary;
        this.hired = hired;
        this.deptId = deptId;
    }

    public int id() {return id;}

    public String name() {return name; }

    public Gender gender() {return gender; };

    public int salary() { return salary; }

    public int hired() { return hired; }

    public int deptId() {return deptId; }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Employee employee = (Employee) o;
        return id == employee.id &&
            salary == employee.salary &&
            hired == employee.hired &&
            deptId == employee.deptId &&
            Objects.equals(name, employee.name) &&
            gender == employee.gender;
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, name, gender, salary, hired, deptId);
    }

    @Override
    public String toString() {
        return "Employee{" +
            "id=" + id +
            ", name='" + name + '\'' +
            ", gender=" + gender +
            ", salary=" + salary +
            ", hired=" + hired +
            ", deptId=" + deptId +
            '}';
    }

    public enum Gender {M, F, O};

    public static Gender[] GENDERS = {Gender.F, Gender.O, Gender.F, Gender.M, Gender.F, Gender.M, Gender.F};
    public static int[] DEPTS = {100, 100, 100, 101, 102, 103, 104};

    public static Stream<Employee> generate() {
        return IntStream.iterate(0, id -> id + 1)
            .mapToObj(id -> {
                final Gender gender = GENDERS[id % GENDERS.length];
                final int dept = DEPTS[id % DEPTS.length];
                final int hired = id % (10 * 365);

                return new Employee(
                    id,
                    "John Doe " + (id % 147),
                    gender,
                    salary(id, gender, hired, dept),
                    hired,
                    dept
                );
            });
    }

    private static int salary(int id, Gender gender, int hired, int dept) {
        return   56_000 +
            id * 147 % 10_000 +
            gender.ordinal() * 1_023 +
            (dept - 100) * 3_512 +
            (hired / 365) * 1_000;
    }




}
