package com.backtype.support;

import com.backtype.hadoop.formats.RecordInputStream;
import com.backtype.hadoop.formats.RecordOutputStream;
import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;
import com.google.common.collect.TreeMultiset;
import org.junit.Assert;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestUtils {

    private static final String TMP_ROOT = "/tmp/unittests";

    public static void assertArraysEqual(byte[] expected, byte[] result) {
        if (!Arrays.equals(expected, result)) {
            throw new AssertionError("Arrays not equal");
        }
    }

    public static void assertNull(Object val) {
        if(val!=null) {
            throw new AssertionError("val is not null");
        }
    }

    public static void assertNotNull(Object val) {
        if(val==null) {
            throw new AssertionError("val is null");
        }
    }

    public static String getTmpPath(FileSystem fs, String name) throws IOException {
        fs.mkdirs(new Path(TMP_ROOT));
        String full = TMP_ROOT + "/" + name;
        if (fs.exists(new Path(full))) {
            fs.delete(new Path(full), true);
        }
        return full;
    }

    public static void deletePath(FileSystem fs, String path) throws IOException {
        fs.delete(new Path(path), true);
    }

    public static void emitToPail(Pail pail, String file, Iterable<String> records) throws IOException {
        RecordOutputStream os = pail.openWrite(file);
        for (String s: records) {
            os.writeRaw(s.getBytes());
        }
        os.close();
    }

    public static void emitToPail(Pail pail, String file, String... records) throws IOException {
        RecordOutputStream os = pail.openWrite(file);
        for (String s: records) {
            os.writeRaw(s.getBytes());
        }
        os.close();
    }

    @SafeVarargs
    public static <T> void emitObjectsToPail(Pail<T> pail, T... records) throws IOException {
        Pail<T>.TypedRecordOutputStream os = pail.openWrite();
        for(T r: records) {
            os.writeObject(r);
        }
        os.close();
    }

    public static <T> void emitObjectsToPail(Pail<T> pail, List<T> records) throws IOException {
        Pail<T>.TypedRecordOutputStream os = pail.openWrite();
        for(T r: records) {
            os.writeObject(r);
        }
        os.close();
    }


    public static List<String> getPailRecords(Pail pail) throws IOException {
        List<String> ret = new ArrayList<String>();
        for(String s: pail.getUserFileNames()) {
            RecordInputStream is = pail.openRead(s);
            while(true) {
                byte[] r = is.readRawRecord();
                if(r==null) break;
                ret.add(new String(r));
            }
            is.close();
        }
        return ret;
    }

    @SafeVarargs
    public static <T extends Comparable> void assertPailContents(Pail<T> pail, T... objects) {
        TreeMultiset<T> contains = getPailContents(pail);
        TreeMultiset<T> other = TreeMultiset.create();
        other.addAll(Arrays.asList(objects));
        Assert.assertEquals(failureString(other, contains), other, contains);
    }

    public static<T> String failureString(Iterable<T> expected, Iterable<T> got) {
        StringBuilder ret = new StringBuilder("\n\nExpected:\n");
        for(T o: expected) {
            ret.append(o.toString()).append("\n\n");
        }
        ret.append("\nGot\n");
        for(T o: got) {
            ret.append(o.toString()).append("\n\n");
        }
        ret.append("\n\n");
        return ret.toString();
    }

    public static <T extends Comparable> void assertPailContents(Pail<T> pail, List<T> objects) {
        TreeMultiset<T> contains = getPailContents(pail);
        TreeMultiset<T> other = TreeMultiset.create();
        other.addAll(objects);
        Assert.assertEquals(failureString(other, contains), other, contains);
    }


    public static <T extends Comparable> TreeMultiset<T> getPailContents(Pail<T> pail) {
        TreeMultiset<T> contains = TreeMultiset.create();
        for(T obj: pail) {
            contains.add(obj);
        }
        return contains;
    }
}
