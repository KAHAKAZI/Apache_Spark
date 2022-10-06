package com.virtualpairprogrammers;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import java.io.*;

public class UtilTest {

    @Test
    public void shouldLoadCorrectAmountOfData() {
        Long size = 10462L;

        InputStream is = Util.class.getResourceAsStream("/subtitles/boringwords.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        Long actualSize = br.lines().count();
        Assertions.assertEquals(actualSize, size);
    }


}