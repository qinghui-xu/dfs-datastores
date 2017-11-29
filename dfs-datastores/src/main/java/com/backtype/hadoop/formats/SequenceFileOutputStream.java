package com.backtype.hadoop.formats;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.IOException;

public class SequenceFileOutputStream implements RecordOutputStream {

    private Writer _writer;
    private BytesWritable writable = new BytesWritable();

    public SequenceFileOutputStream(FileSystem fs, Path path) throws IOException {
        _writer = SequenceFile.createWriter(fs.getConf(), Writer.file(path), Writer.keyClass(BytesWritable.class),
                Writer.valueClass(NullWritable.class), Writer.compression(CompressionType.NONE));
    }

    public SequenceFileOutputStream(FileSystem fs, Path path, CompressionType type, CompressionCodec codec) throws IOException {
        _writer = SequenceFile.createWriter(fs.getConf(), Writer.file(path), Writer.keyClass(BytesWritable.class),
                Writer.valueClass(NullWritable.class), Writer.compression(type, codec));
    }

    public void writeRaw(byte[] record) throws IOException {
        writeRaw(record, 0, record.length);
    }

    public void writeRaw(byte[] record, int start, int length) throws IOException {
        writable.set(record, start, length);
        _writer.append(writable, NullWritable.get());
    }


    public void close() throws IOException {
        _writer.close();
    }

    public long getPos() throws IOException {
        return  _writer.getLength();
    }

}
