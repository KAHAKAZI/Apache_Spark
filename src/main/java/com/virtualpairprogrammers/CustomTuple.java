package com.virtualpairprogrammers;

import java.io.Serializable;

public class CustomTuple implements Comparable<CustomTuple>, Serializable {

    private Long count;

    private String value;

    public CustomTuple(Long count, String value) {
        this.count = count;
        this.value = value;
    }

    @Override
    public int compareTo(CustomTuple o) {
        if (o.count == count) {
            return 0;
        } else if (count > o.count) {
            return 1;
        } else {
            return -1;
        }
    }

    @Override
    public String toString() {
        return count + ", " + value;
    }
}
