package com.backtype.hadoop.pail;

import com.backtype.hadoop.PathLister;
import com.backtype.hadoop.formats.RecordInputStream;
import com.backtype.hadoop.formats.RecordOutputStream;
import com.backtype.hadoop.formats.RecordStreamFactory;
import com.backtype.support.SubsetSum;
import com.backtype.support.SubsetSum.Value;
import com.backtype.support.Utils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;


public class Consolidator {
    public static final long DEFAULT_CONSOLIDATION_SIZE = 1024*1024*127; //127 MB
    public static final String CONSOLIDATION_PAIL_PATH_KEY = "consolidation.pail.path";
    public static final String CONSOLIDATION_PLAN_METADATA_NAME = "consolidation_plan";
    public static final String ARGS = "consolidator_args";
    public static final String CONSOLIDATION_SUCCESS = "consolidation_success";

    private static final Logger LOG = LoggerFactory.getLogger(Consolidator.class);

    private static Thread shutdownHook;
    private static RunningJob job = null;

    public static class ConsolidatorArgs implements Serializable {
        public String instanceRoot;
        public String fsUri;
        public RecordStreamFactory streams;
        public PathLister pathLister;
        public long targetSizeBytes;
        public String extension;

        public ConsolidatorArgs(String fsUri, RecordStreamFactory streams, PathLister pathLister, long targetSizeBytes,
                                String instanceRoot, String extension) {
            this.fsUri = fsUri;
            this.streams = streams;
            this.pathLister = pathLister;
            this.targetSizeBytes = targetSizeBytes;
            this.extension = extension;
            this.instanceRoot = instanceRoot;
        }
    }

    private static String getDirsString(List<String> targetDirs) {
        String ret = "";
        for(int i=0; i < 3 && i < targetDirs.size(); i++) {
            ret+=(targetDirs.get(i) + " ");
        }
        if(targetDirs.size()>3) {
            ret+="...";
        }
        return ret;
    }

    private static List<String> getPartitionsToConsolidate(Pail thePail, String extension) throws IOException {
        List<String> toCheck = new ArrayList<String>();
        toCheck.add("");
        PailStructure structure = thePail.getSpec().getStructure();
        List<String> partitionsToConsolidate = new ArrayList<String>();
        while(toCheck.size()>0) {
            String dir = toCheck.remove(0);
            List<String> dirComponents = thePail.componentsFromRoot(dir);
            if(structure.isValidTarget(dirComponents.toArray(new String[dirComponents.size()]))) {
                partitionsToConsolidate.add(thePail.toFullPath(dir));
            } else {
                for(FileStatus fstat:thePail.listStatus(new Path(thePail.toFullPath(dir)))) {
                    if(fstat.isDirectory()) {
                        toCheck.add(fstat.getPath().toString());
                    } else {
                        if (fstat.getPath().getName().endsWith(extension)) {
                            throw new IllegalStateException(fstat.getPath().toString() + " is not a dir and breaks the structure of " + thePail.getInstanceRoot());
                        }
                    }
                }
            }
        }
        return partitionsToConsolidate;
    }

    public static void consolidate(FileSystem fs, Pail thePail, RecordStreamFactory streams, PathLister lister,
                                   long targetSizeBytes, String extension) throws IOException {

        // prevent 2 running instances running at the same time
        // add the ability to recover from failure

        JobConf conf = new JobConf(fs.getConf(), Consolidator.class);
        String fsUri = fs.getUri().toString();
        ConsolidatorArgs args = new ConsolidatorArgs(fsUri, streams, lister, targetSizeBytes, thePail.getInstanceRoot(), extension);
        Utils.setObject(conf, ARGS, args);

        conf.setJobName("Consolidator: " + thePail.getInstanceRoot());

        conf.setInputFormat(ConsolidatorInputFormat.class);
        conf.setOutputFormat(NullOutputFormat.class);
        conf.setOutputCommitter(ConsolidatorOutputCommitter.class);
        conf.set(CONSOLIDATION_PAIL_PATH_KEY, thePail.getInstanceRoot());
        conf.setMapperClass(ConsolidatorMapper.class);

        conf.setSpeculativeExecution(false);

        conf.setNumReduceTasks(0);

        conf.setOutputKeyClass(NullWritable.class);
        conf.setOutputValueClass(NullWritable.class);

        conf.unset("mapred.max.map.failures.percent ");
        conf.unset("mapreduce.map.failures.maxpercent");
        conf.unset("mapreduce.map.maxattempts");

        try {
            registerShutdownHook();
            job = new JobClient(conf).submitJob(conf);

            while(!job.isComplete()) {
                Thread.sleep(100);
            }
            if(!job.isSuccessful()) throw new IOException("Consolidator failed");
            deregisterShutdownHook();
        } catch(IOException e) {

            IOException ret = new IOException("Consolidator failed");
            ret.initCause(e);
            throw ret;
        } catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void registerShutdownHook() {
        shutdownHook = new Thread()
        {
            @Override
            public void run()
            {
                try {
                    if(job != null)
                        job.killJob();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        Runtime.getRuntime().addShutdownHook( shutdownHook );
    }

    private static void deregisterShutdownHook()
    {
        Runtime.getRuntime().removeShutdownHook( shutdownHook );
    }

    public static class ConsolidatorMapper extends MapReduceBase implements Mapper<ArrayWritable, Text, NullWritable, NullWritable> {
        public static Logger LOG = LoggerFactory.getLogger(ConsolidatorMapper.class);

        FileSystem fs;
        ConsolidatorArgs args;

        public void map(ArrayWritable sourcesArr, Text target, OutputCollector<NullWritable, NullWritable> oc, Reporter rprtr) throws IOException {

            Path finalFile = new Path(target.toString());

            List<Path> sources = new ArrayList<Path>();
            for(int i=0; i<sourcesArr.get().length; i++) {
                sources.add(new Path(((Text)sourcesArr.get()[i]).toString()));
            }
            //must have failed after succeeding to create file but before task finished - this is valid
            //because path is selected with a UUID
            if(!fs.exists(finalFile)) {
                Path tmpFile = new Path(finalFile.getParent(), "_"+UUID.randomUUID().toString());
                fs.mkdirs(tmpFile.getParent());

                String status = "Consolidating 0/" + sources.size() + " files into " + tmpFile.toString();
                LOG.info(status);
                rprtr.setStatus(status);

                RecordStreamFactory fact = args.streams;
                fs.mkdirs(finalFile.getParent());

                RecordOutputStream os = fact.getOutputStream(fs, tmpFile);
                for (int i = 0; i < sources.size(); i++) {
                    Path p = sources.get(i);
                    LOG.info("Opening " + p.toString() + " for consolidation");
                    status = "Consolidating "+(i+1)+"/" + sources.size() + " files into " + tmpFile.toString();
                    rprtr.setStatus(status);
                    RecordInputStream is = fact.getInputStream(fs, p);
                    byte[] record;
                    while((record = is.readRawRecord()) != null) {
                        os.writeRaw(record);
                    }
                    is.close();
                    rprtr.progress();
                }
                os.close();

                status = "Renaming " + tmpFile.toString() + " to " + finalFile.toString();
                LOG.info(status);
                rprtr.setStatus(status);

                if(!fs.rename(tmpFile, finalFile))
                    throw new IOException("could not rename " + tmpFile.toString() + " to " + finalFile.toString());
            }

            String status = "Deleting " + sources.size() + " original files";
            LOG.info(status);
            rprtr.setStatus(status);

            for(Path p: sources) {
                try {
                    fs.delete(p, false);
                } catch (FileNotFoundException e) {
                    LOG.info("file " + p + " already deleted");
                }
                rprtr.progress();
            }

        }

        @Override
        public void configure(JobConf conf) {
            args = (ConsolidatorArgs) Utils.getObject(conf, ARGS);
            try {
                fs = Utils.getFS(args.fsUri, conf);
            } catch(IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class ConsolidatorSplit implements InputSplit {
        public String[] sources;
        public String target;

        public ConsolidatorSplit() {

        }

        public ConsolidatorSplit(String[] sources, String target) {
            this.sources = sources;
            this.target = target;
        }

        public ConsolidatorSplit(String[] sources, String target, Long length, String[] locations) {
            this(sources, target);
        }

        public String[] getSources() {
            return sources;
        }

        public String getTarget() {
            return target;
        }

        public void setSources(String[] sources) {
            this.sources = sources;
        }

        public void setTarget(String target) {
            this.target = target;
        }


        public long getLength() throws IOException {
            return 1;
        }

        public String[] getLocations() throws IOException {
            return new String[] {};
        }

        public void write(DataOutput d) throws IOException {
            WritableUtils.writeString(d, target);
            WritableUtils.writeStringArray(d, sources);
        }

        public void readFields(DataInput di) throws IOException {
            target = WritableUtils.readString(di);
            sources = WritableUtils.readStringArray(di);
        }

    }

    public static class ConsolidatorRecordReader implements RecordReader<ArrayWritable, Text> {
        private ConsolidatorSplit split;
        boolean finished = false;

        public ConsolidatorRecordReader(ConsolidatorSplit split) {
            this.split = split;
        }

        public boolean next(ArrayWritable k, Text v) throws IOException {
            if(finished) return false;
            Writable[] sources = new Writable[split.sources.length];
            for(int i=0; i<sources.length; i++) {
                sources[i] = new Text(split.sources[i]);
            }
            k.set(sources);
            v.set(split.target);

            finished = true;
            return true;
        }

        public ArrayWritable createKey() {
            return new ArrayWritable(Text.class);
        }

        public Text createValue() {
            return new Text();
        }

        public long getPos() throws IOException {
            if(finished) return 1;
            else return 0;
        }

        public void close() throws IOException {
        }

        public float getProgress() throws IOException {
            if(finished) return 1;
            else return 0;
        }

    }

    public static class ConsolidatorOutputCommitter extends OutputCommitter {

        public void setupJob(JobContext jobContext) throws IOException {

        }

        @Override
        public void commitJob(JobContext jobContext) throws IOException {
            // job is successful, remove consolidation plan.
            ConsolidatorArgs args = (ConsolidatorArgs) Utils.getObject(jobContext.getJobConf(), ARGS);
            Pail pail = new Pail(args.instanceRoot, jobContext.getConfiguration());
            pail.deleteMetadata(CONSOLIDATION_PLAN_METADATA_NAME);
            pail.writeMetadata(CONSOLIDATION_SUCCESS, "");
        }

        public void setupTask(TaskAttemptContext taskContext) throws IOException {
            // do nothing
        }

        public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
            return false;
        }

        public void commitTask(TaskAttemptContext taskContext) throws IOException {
            // do nothing
        }

        public void abortTask(TaskAttemptContext taskContext) throws IOException {
            // do nothing
        }
    }


    public static class ConsolidatorInputFormat implements InputFormat<ArrayWritable, Text> {
        private static final String TARGET_SOURCE_SEP = "::";
        private static final String SOURCE_SEP = ",";

        private static class PathSizePair implements Value {
            public Path path;
            public long size;

            public PathSizePair(Path p, long s) {
                this.path = p;
                this.size = s;
            }

            public long getValue() {
                return size;
            }

        }

        private List<PathSizePair> getFileSizePairs(FileSystem fs, List<Path> files) throws IOException {
            List<PathSizePair> results = new ArrayList<PathSizePair>();
            for(Path p: files) {
                long size = fs.getContentSummary(p).getLength();
                results.add(new PathSizePair(p, size));
            }
            return results;
        }


        private String[] pathsToStrs(List<PathSizePair> pairs) {
            String[] ret = new String[pairs.size()];
            for(int i=0; i<pairs.size(); i++) {
                ret[i] = pairs.get(i).path.toString();
            }
            return ret;
        }

        private List<ConsolidatorSplit> createSplits(FileSystem fs, List<Path> files,
                String target, long targetSize, String extension) throws IOException {
            List<PathSizePair> working = getFileSizePairs(fs, files);
            List<ConsolidatorSplit> ret = new ArrayList<ConsolidatorSplit>();
            List<List<PathSizePair>> splits = SubsetSum.split(working, targetSize);
            for(List<PathSizePair> c: splits) {
                if(c.size()>1) {
                    String rand = UUID.randomUUID().toString();
                    String targetFile = new Path(target,
                        "" + rand.charAt(0) + rand.charAt(1) + "/cons" +
                        rand + extension).toString();
                    ret.add(new ConsolidatorSplit(pathsToStrs(c), targetFile));

                }
            }
            Collections.sort(ret, new Comparator<InputSplit>() {
                public int compare(InputSplit o1, InputSplit o2) {
                    return ((ConsolidatorSplit)o2).sources.length - ((ConsolidatorSplit)o1).sources.length;
                }
            });
            return ret;
        }

        private ConsolidatorSplit[] readSplitFromPreviousPlan(Pail thePail, String plan) throws IOException {
            List<ConsolidatorSplit> splits = new ArrayList<ConsolidatorSplit>();
            for (String mapping : plan.split("\n")) {
                String[] parsed = mapping.split(TARGET_SOURCE_SEP);
                String target = parsed[0];
                String[] sources = parsed[1].split(SOURCE_SEP);
                splits.add(new ConsolidatorSplit(sources, target));
            }
            return splits.toArray(new ConsolidatorSplit[splits.size()]);
        }

        private void writePlanToPail(List<ConsolidatorSplit> splits, Pail thePail) throws IOException {
            StringBuilder consolidatePlan = new StringBuilder();

            for(ConsolidatorSplit split : splits) {
                consolidatePlan.append(split.target).append(TARGET_SOURCE_SEP);
                consolidatePlan.append(split.sources[0]);
                for (int i = 1; i < split.sources.length; i++) {
                    consolidatePlan.append(SOURCE_SEP).append(split.sources[i]);
                }
                consolidatePlan.append("\n");
            }

            thePail.writeMetadata(CONSOLIDATION_PLAN_METADATA_NAME, consolidatePlan.toString());
        }

        public InputSplit[] getSplits(JobConf conf, int ignored) throws IOException {
            ConsolidatorArgs args = (ConsolidatorArgs) Utils.getObject(conf, ARGS);
            PathLister lister = args.pathLister;
            Pail thePail = new Pail(args.instanceRoot, conf);

            String oldPlan = thePail.getMetadata(CONSOLIDATION_PLAN_METADATA_NAME);
            if(oldPlan != null) {
                return readSplitFromPreviousPlan(thePail, oldPlan);
            } else {
                List<ConsolidatorSplit> ret = new ArrayList<ConsolidatorSplit>();
                List<String> dirs = getPartitionsToConsolidate(thePail, args.extension);
                for (String dir : dirs) {
                    FileSystem fs = Utils.getFS(dir, conf);
                    List<ConsolidatorSplit> splits = createSplits(fs, lister.getFiles(fs, dir),
                            dir, args.targetSizeBytes, args.extension);
                    ret.addAll(splits);
                }
                writePlanToPail(ret, thePail);
                return ret.toArray(new InputSplit[ret.size()]);
            }
        }

        public RecordReader<ArrayWritable, Text> getRecordReader(InputSplit is, JobConf jc, Reporter rprtr) throws IOException {
            return new ConsolidatorRecordReader((ConsolidatorSplit) is);
        }
    }
}
