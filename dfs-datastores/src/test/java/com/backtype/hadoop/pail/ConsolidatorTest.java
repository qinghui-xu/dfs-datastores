package com.backtype.hadoop.pail;

import com.backtype.hadoop.Consolidator;
import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.PailFormatFactory;
import com.backtype.hadoop.pail.TestStructure;
import com.backtype.support.FSTestCase;
import com.backtype.support.Utils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.backtype.hadoop.pail.PailOpsTest.readWithIt;
import static com.backtype.hadoop.pail.PailOpsTest.writeStrings;
import static com.backtype.support.TestUtils.assertPailContents;
import static com.backtype.support.TestUtils.getTmpPath;

public class ConsolidatorTest extends FSTestCase {

    public void testConsolidationOne() throws Exception {
        String path = getTmpPath(local, "pail");
        Pail pail = Pail.create(local, path);
        writeStrings(pail, "aaa", "a", "b", "c", "d", "e");
        writeStrings(pail, "b/c/ddd", "1", "2", "3");
        writeStrings(pail, "b/c/eee", "aaa", "bbb", "ccc", "ddd", "eee", "fff");
        writeStrings(pail, "f", "z");
        writeStrings(pail, "g", "zz");
        writeStrings(pail, "h", "zzz");
        pail.writeMetadata("a/b/qqq", "lalala");
        pail.writeMetadata("f", "abc");
        pail.consolidate();
        assertEquals(1, pail.getUserFileNames().size());
        Set<String> results = new HashSet<String>(readWithIt(pail));
        Set<String> expected = new HashSet<String>(Arrays.asList("a", "b", "c", "d", "e",
                "1", "2", "3","aaa", "bbb", "ccc", "ddd", "eee", "fff","z", "zz", "zzz"));
        assertEquals(expected, results);
        assertEquals("abc", pail.getMetadata("f"));
        assertEquals("lalala", pail.getMetadata("a/b/qqq"));
    }

    public void testConsolidationMany() throws Exception {
        String path = getTmpPath(local, "pail");
        Pail pail = Pail.create(local, path);
        writeStrings(pail, "aaa", "a", "b", "c", "d", "e");
        writeStrings(pail, "b/c/ddd", "1", "2", "3");
        writeStrings(pail, "b/c/eee", "aaa", "bbb", "ccc", "ddd", "eee", "fff");
        writeStrings(pail, "f", "z");
        writeStrings(pail, "g", "zz");
        writeStrings(pail, "h", "zzz");
        long target = local.getContentSummary(pail.toStoredPath("f")).getLength() +
                local.getContentSummary(pail.toStoredPath("g")).getLength() + 1;
        pail.consolidate(target);
        assertTrue(pail.getUserFileNames().size() < 6 && pail.getUserFileNames().size() > 1);
        Set<String> results = new HashSet<String>(readWithIt(pail));
        Set<String> expected = new HashSet<String>(Arrays.asList("a", "b", "c", "d", "e",
                "1", "2", "3","aaa", "bbb", "ccc", "ddd", "eee", "fff","z", "zz", "zzz"));
        assertEquals(expected, results);
    }

    public void testConsolidateStructured() throws Exception {
        String path = getTmpPath(fs, "pail");
        Pail<String> pail = Pail.create(fs, path, PailFormatFactory.getDefaultCopy().setStructure(new TestStructure()));
        Pail<String>.TypedRecordOutputStream os = pail.openWrite();
        os.writeObject("a1");
        os.writeObject("b1");
        os.writeObject("c1");
        os.writeObject("a2");
        os.writeObject("za1");
        os.writeObject("za2");
        os.writeObject("zb1");
        os.close();
        os = pail.openWrite();
        os.writeObject("a7");
        os.writeObject("a8");
        os.writeObject("za3");
        os.close();
        pail.consolidate();
        assertPailContents(pail, "a1", "b1", "c1", "a2", "za1", "za2", "zb1", "a7", "a8", "za3");
        assertPailContents(pail.getSubPail("a"), "a1", "a2", "a7", "a8");
        assertPailContents(pail.getSubPail("z"), "za1", "za2", "zb1", "za3");
        assertPailContents(pail.getSubPail("z/a"), "za1", "za2", "za3");
    }

    public void testConsolidateRecovery() throws Exception {
        String path = getTmpPath(fs, "pail");
        Pail<String> pail = Pail.create(fs, path, PailFormatFactory.getDefaultCopy().setStructure(new TestStructure()));
        Pail<String>.TypedRecordOutputStream os = pail.openWrite();
        os.writeObject("a1");
        os.writeObject("b1");
        os.writeObject("c1");
        os.writeObject("a2");
        os.writeObject("za1");
        os.writeObject("za2");
        os.writeObject("zb1");
        os.close();
        os = pail.openWrite();
        os.writeObject("a7");
        os.writeObject("a8");
        os.writeObject("za3");
        os.close();

        Consolidator.ConsolidatorInputFormat cif = new Consolidator.ConsolidatorInputFormat();
        Consolidator.ConsolidatorArgs args = new Consolidator.ConsolidatorArgs(
                fs.getUri().toString(), pail.getFormat(), new Pail.PailPathLister(false),
                128 * 1024 * 1024, pail.getInstanceRoot(), pail.EXTENSION);

        JobConf jc = new JobConf();
        Utils.setObject(jc, Consolidator.ARGS, args);

        cif.getSplits(jc, 0);

        assertTrue(fs.exists(new Path(path, Consolidator.CONSOLIDATION_PLAN_METADATA_NAME +Pail.META_EXTENSION)));

        pail.consolidate();

        assertFalse(fs.exists(new Path(path, Consolidator.CONSOLIDATION_PLAN_METADATA_NAME +Pail.META_EXTENSION)));

        assertPailContents(pail, "a1", "b1", "c1", "a2", "za1", "za2", "zb1", "a7", "a8", "za3");
        assertPailContents(pail.getSubPail("a"), "a1", "a2", "a7", "a8");
        assertPailContents(pail.getSubPail("z"), "za1", "za2", "zb1", "za3");
        assertPailContents(pail.getSubPail("z/a"), "za1", "za2", "za3");
    }
}
