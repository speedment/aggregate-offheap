package com.speedment.aggregate;

public class HelloWorld {

    public static void main(String... args) {
        Employee.generate()
            .limit(10)
            .forEach(System.out::println);
    }
}
