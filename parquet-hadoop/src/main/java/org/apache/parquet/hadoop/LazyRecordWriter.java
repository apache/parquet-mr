package org.apache.parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/***************TSP**************/
// Delete output dir in case write empty records.
public class LazyRecordWriter<T> extends RecordWriter<NullWritable, T>
{
    private static final Logger LOG = LoggerFactory.getLogger(ParquetOutputFormat.class);

    private Configuration conf;

    private Path file;

    private CompressionCodecName codec;

    private WriteSupport<T> writeSupport;

    private ParquetRecordWriter<T> realWriter = null;

    private boolean initialized = false;

    public LazyRecordWriter(Configuration conf, Path file, CompressionCodecName codec, WriteSupport<T> writeSupport)
    {
        this.conf = conf;
        this.file = file;
        this.codec = codec;
        this.writeSupport = writeSupport;
        this.initialized = false;
    }

    private synchronized void lazyInitialize()
        throws IOException
    {
        ParquetProperties props = ParquetProperties.builder()
            .withPageSize(ParquetOutputFormat.getPageSize(conf))
            .withDictionaryPageSize(ParquetOutputFormat.getDictionaryPageSize(conf))
            .withDictionaryEncoding(ParquetOutputFormat.getEnableDictionary(conf))
            .withWriterVersion(ParquetOutputFormat.getWriterVersion(conf))
            .estimateRowCountForPageSizeCheck(ParquetOutputFormat.getEstimatePageSizeCheck(conf))
            .withMinRowCountForPageSizeCheck(ParquetOutputFormat.getMinRowCountForPageSizeCheck(conf))
            .withMaxRowCountForPageSizeCheck(ParquetOutputFormat.getMaxRowCountForPageSizeCheck(conf))
            .build();

        long blockSize = ParquetOutputFormat.getLongBlockSize(conf);
        int maxPaddingSize = ParquetOutputFormat.getMaxPaddingSize(conf);
        boolean validating = ParquetOutputFormat.getValidation(conf);

        if (LOG.isInfoEnabled())
        {
            LOG.info("Parquet block size to {}", blockSize);
            LOG.info("Parquet page size to {}", props.getPageSizeThreshold());
            LOG.info("Parquet dictionary page size to {}", props.getDictionaryPageSizeThreshold());
            LOG.info("Dictionary is {}", (props.isEnableDictionary() ? "on" : "off"));
            LOG.info("Validation is {}", (validating ? "on" : "off"));
            LOG.info("Writer version is: {}", props.getWriterVersion());
            LOG.info("Maximum row group padding size is {} bytes", maxPaddingSize);
            LOG.info("Page size checking is: {}", (props.estimateNextSizeCheck() ? "estimated" : "constant"));
            LOG.info("Min row count for page size check is: {}", props.getMinRowCountForPageSizeCheck());
            LOG.info("Max row count for page size check is: {}", props.getMaxRowCountForPageSizeCheck());
        }

        WriteContext init = writeSupport.init(conf);
        ParquetFileWriter w = new ParquetFileWriter(
            conf, init.getSchema(), file, ParquetFileWriter.Mode.CREATE, blockSize, maxPaddingSize);
        w.start();

        float maxLoad = conf.getFloat(ParquetOutputFormat.MEMORY_POOL_RATIO,
            MemoryManager.DEFAULT_MEMORY_POOL_RATIO);
        long minAllocation = conf.getLong(ParquetOutputFormat.MIN_MEMORY_ALLOCATION,
            MemoryManager.DEFAULT_MIN_MEMORY_ALLOCATION);
        synchronized (ParquetOutputFormat.class)
        {
            if (ParquetOutputFormat.getMemoryManager() == null)
            {
                ParquetOutputFormat.setMemoryManager(new MemoryManager(maxLoad, minAllocation));
            }
        }
        if (ParquetOutputFormat.getMemoryManager().getMemoryPoolRatio() != maxLoad)
        {
            LOG.warn("The configuration " + ParquetOutputFormat.MEMORY_POOL_RATIO + " has been set. It should not " +
                "be reset by the new value: " + maxLoad);
        }

        realWriter = new ParquetRecordWriter<T>(
            w,
            writeSupport,
            init.getSchema(),
            init.getExtraMetaData(),
            blockSize,
            codec,
            validating,
            props,
            ParquetOutputFormat.getMemoryManager(),
            conf);
        initialized = true;
    }

    @Override
    public void close(TaskAttemptContext context)
        throws IOException, InterruptedException
    {
        if (initialized)
        {
            realWriter.close(context);
        }
    }

    @Override
    public void write(NullWritable key, T value)
        throws IOException, InterruptedException
    {
        if (!initialized)
        {
            lazyInitialize();
        }
        realWriter.write(key, value);
    }
}
/********************************/