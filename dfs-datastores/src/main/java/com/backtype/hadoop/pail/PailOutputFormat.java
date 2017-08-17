package com.backtype.hadoop.pail;

import com.backtype.hadoop.formats.RecordOutputStream;
import com.backtype.support.Utils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class  PailOutputFormat extends FileOutputFormat<Text, BytesWritable> {
    public static Logger LOG = LoggerFactory.getLogger(PailOutputFormat.class);
    public static final String SPEC_ARG = "pail_spec_arg";

    // we want to have ~512MB files in output
    // if compression is enabled, it can buffer 1MB of data by default before compression
    // so 512MB - 2MB = 500MB
    public static final long DEFAULT_FILE_LIMIT_SIZE_BYTES = 512L * 1024 * 1024; // 510MB
    public static final String FILE_LIMIT_SIZE_BYTES_KEY = "pail.output.file.limit.size.bytes";

    /**
     * Change this to just use Pail#writeObject - automatically fix up BytesWritable
     */
    public static class PailRecordWriter implements RecordWriter<Text, BytesWritable> {
        private Pail _pail;
        private String _unique;
        private final long fileLimitSizeBytes;

        protected static class OpenAttributeFile {
            public String attr;
            public String filename;
            public RecordOutputStream os;
            public long numBytesWritten = 0;

            public OpenAttributeFile(String attr, String filename, RecordOutputStream os) {
                this.attr = attr;
                this.filename = filename;
                this.os = os;
            }
        }

        private Map<String, OpenAttributeFile> _outputters = new HashMap<String, OpenAttributeFile>();
        private int writtenRecords = 0;
        private int numFilesOpened = 0;

        public PailRecordWriter(JobConf conf, String unique, Progressable p) throws IOException {
            int ioSeqfileCompressBlocksize = conf.getInt("io.seqfile.compress.blocksize", 1000000);
            fileLimitSizeBytes = conf.getLong(PailOutputFormat.FILE_LIMIT_SIZE_BYTES_KEY, DEFAULT_FILE_LIMIT_SIZE_BYTES - 2 * ioSeqfileCompressBlocksize);
            PailSpec spec = (PailSpec) Utils.getObject(conf, SPEC_ARG);

            Path path = getOutputPath(conf);
            FileSystem fs = path.getFileSystem(conf);

            Pail.create(fs, path.toString(), spec,  false);
            // this is a hack to get the work output directory since it's not exposed directly. instead it only
            // provides a path to a particular file.
            _pail = Pail.create(fs, FileOutputFormat.getTaskOutputPath(conf, unique).getParent().toString(), spec, false);
            _unique = unique;
        }

        public void write(Text k, BytesWritable v) throws IOException {
            String attr = k.toString();
            OpenAttributeFile oaf = _outputters.get(attr);
            if (oaf != null && (oaf.os.getPos() >= fileLimitSizeBytes ||
                    oaf.os.getPos() == -1L && oaf.numBytesWritten >= fileLimitSizeBytes)) {
                // fetch the actual number of bytes written physically on the FileSystem by getting the offset of the file
                // or if offset is not supported by the writer, get the number of bytes sent to the writer.
                // if this number is greater than a threshold set via conf key pail.output.file.limit.size.bytes and with a default of 1GB,
                // then close current file and open a new one.
                closeAttributeFile(oaf);
                oaf = null;
                _outputters.remove(attr);
            }
            if(oaf==null) {
                String filename;
                if(!attr.isEmpty()) {
                    filename = attr + "/" + _unique + numFilesOpened;
                } else {
                    filename = _unique + numFilesOpened;
                }
                numFilesOpened++;
                LOG.info("Opening " + filename + " for attribute " + attr);
                //need overwrite for situations where regular FileOutputCommitter isn't used (like S3)
                oaf = new OpenAttributeFile(attr, filename, _pail.openWrite(filename, true));
                _outputters.put(attr, oaf);
            }
            oaf.os.writeRaw(v.getBytes(), 0, v.getLength());
            oaf.numBytesWritten+=v.getLength();
            logProgress();
        }

        /**
         * Log information every 10000th record written.
         * Log the bytes written to the stream and the actual bytes written on disk if available.
         */
        protected void logProgress() {
            writtenRecords++;
            if(writtenRecords%10000 == 0) {
                for(OpenAttributeFile oaf: _outputters.values()) {

                    try {
                        long pos = oaf.os.getPos();
                        LOG.info("Attr:" + oaf.attr + " Filename:" + oaf.filename + " Bytes written:" + oaf.numBytesWritten + " " + pos);
                    } catch (IOException e) {
                        LOG.info("Attr:" + oaf.attr + " Filename:" + oaf.filename + " Bytes written:" + oaf.numBytesWritten);
                    }
                }
            }
        }

        protected void closeAttributeFile(OpenAttributeFile oaf) throws IOException {
            LOG.info("Closing " + oaf.filename + " for attr " + oaf.attr);
            //print out the size of the file here
            oaf.os.close();
            LOG.info("Closed " + oaf.filename + " for attr " + oaf.attr);
        }

        public void close(Reporter rprtr) throws IOException {
            for(String key: _outputters.keySet()) {
                closeAttributeFile(_outputters.get(key));
                rprtr.progress();
            }
            _outputters.clear();
        }
    }

    public RecordWriter<Text, BytesWritable> getRecordWriter(FileSystem ignored, JobConf jc, String string, Progressable p) throws IOException {
        return new PailRecordWriter(jc, string, p);
    }

    @Override
    public void checkOutputSpecs(FileSystem fs, JobConf conf) throws IOException {
        // because this outputs multiple files, doesn't work with speculative execution on something like EMR with S3
        if(!(conf.getOutputCommitter() instanceof FileOutputCommitter)) {
            if(conf.getMapSpeculativeExecution() && conf.getNumReduceTasks()==0 || conf.getReduceSpeculativeExecution()) {
                throw new IllegalArgumentException("Cannot use speculative execution with PailOutputFormat unless FileOutputCommitter is enabled");
            }
        }
    }

}
