package com.facebook.presto.tests.sqllogictest;

import org.weakref.jmx.internal.guava.hash.HashCode;
import org.weakref.jmx.internal.guava.hash.HashFunction;
import org.weakref.jmx.internal.guava.hash.Hasher;
import org.weakref.jmx.internal.guava.hash.Hashing;

import javax.xml.bind.DatatypeConverter;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

public class SqlTestMain
{
    public static void main(String[] args)
            throws Exception
    {
        SqlTestMain sqlTestMain = new SqlTestMain();
        sqlTestMain.test();
    }

    private void test()
            throws Exception
    {
        Hasher hasher = Hashing.md5().newHasher();
        MessageDigest md = MessageDigest.getInstance("MD5");
        List<Integer> integers = Arrays.asList(358, 364, 376, 382, 398, 402, 410, 426, 432, 440, 458, 468, 478, 486, 490, 1000, 1050, 1120, 1180, 1240, 1290, 1300, 1390, 1430, 1450, 1510, 1580, 1600, 1670, 1700);
        for (Integer integer : integers) {
            hasher.putString(integer + "\n", StandardCharsets.UTF_8);
            md.update(integer.byteValue());
        }
        /**
         * Expected :3c13dee48d9356ae19af2515e05e6b54
         * Actual   :240879f50204c2bd867ff3b3f8296f49
         */
        System.out.println(hasher.hash().toString());

        System.out.println(DatatypeConverter.printHexBinary(md.digest()));
    }
}
