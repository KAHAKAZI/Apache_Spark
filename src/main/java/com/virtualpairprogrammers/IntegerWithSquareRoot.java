package com.virtualpairprogrammers;

public class IntegerWithSquareRoot {
    private Integer originalInteger;
    private Double squareRoot;
    public IntegerWithSquareRoot(Integer value) {
        this.originalInteger = value;
        this.squareRoot = Math.sqrt(value);
    }

    @Override
    public String toString() {
        return originalInteger + ", " + squareRoot;
    }
}
