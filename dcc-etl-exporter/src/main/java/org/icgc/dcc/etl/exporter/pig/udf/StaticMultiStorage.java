package org.icgc.dcc.etl.exporter.pig.udf;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

import lombok.Cleanup;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreMetadata;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.StorageUtil;
import org.apache.pig.impl.util.UDFContext;

import com.google.common.base.Preconditions;

/**
 * THIS CLASS IS ADOPTED FROM PIGGYBANK MultiStorage
 * 
 */
@Slf4j
public class StaticMultiStorage extends StoreFunc implements StoreMetadata {

  private static final String DOWNLOADER_FIELD_KEY_INDEX_PROPERTY = "downloader.field.key.index";

  private static final String HEADER = "-header";

  private static final String PREFIX = "dcc-";

  private Path outputPath; // User specified output Path
  private int splitFieldIndex = -1; // Index of the key field
  private String splitFieldKey;
  private String fieldDel; // delimiter of the output record.
  private Compression comp; // Compression type of output data.
  private String dataTypeLongName;

  // Compression types supported by this store
  enum Compression {
    none, bz2, bz, gz;
  };

  public StaticMultiStorage(String parentPathStr, String splitFieldKey) {
    this(parentPathStr, splitFieldKey, "none");
  }

  public StaticMultiStorage(String parentPathStr, String splitFieldKey,
      String compression) {
    this(parentPathStr, splitFieldKey, "none", compression, "\\t");
  }

  /**
   * Constructor
   * 
   * @param parentPathStr Parent output dir path
   * @param splitFieldIndex key field index
   * @param compression 'bz2', 'bz', 'gz' or 'none'
   * @param fieldDel Output record field delimiter.
   */
  public StaticMultiStorage(String parentPathStr, String dataTypeLongName, String splitFieldKey,
      String compression, String fieldDel) {
    this.outputPath = new Path(parentPathStr);
    this.splitFieldKey = splitFieldKey;
    this.fieldDel = fieldDel;
    this.dataTypeLongName = dataTypeLongName;
    try {
      this.comp = (compression == null) ? Compression.none : Compression
          .valueOf(compression.toLowerCase());
    } catch (IllegalArgumentException e) {
      System.err.println("Exception when converting compression string: "
          + compression + " to enum. No compression will be used");
      this.comp = Compression.none;
    }
  }

  private RecordWriter<String, Tuple> writer;

  private String udfcSignature;

  @Override
  public void putNext(Tuple tuple) throws IOException {
    if (tuple.size() <= splitFieldIndex) {
      throw new IOException("split field index:" + this.splitFieldIndex
          + " >= tuple size:" + tuple.size());
    }
    Object field = null;
    try {
      field = tuple.get(splitFieldIndex);
    } catch (ExecException exec) {
      throw new IOException(exec);
    }
    try {
      writer.write(String.valueOf(field), tuple);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  public OutputFormat getOutputFormat() throws IOException {
    MultiStorageOutputFormat format = new MultiStorageOutputFormat();
    format.setKeyValueSeparator(fieldDel);
    return format;
  }

  @Override
  public void checkSchema(ResourceSchema schema) throws IOException {
    // TODO: Dictionary check
    String[] fieldNames = schema.fieldNames();
    Preconditions.checkNotNull(fieldNames);

    for (int i = 0; i < fieldNames.length; ++i) {
      if (fieldNames[i].equals(this.splitFieldKey)) {
        this.splitFieldIndex = i;
      }
    }
    if (this.splitFieldIndex == -1) {
      throw new RuntimeException("Key :" + this.splitFieldKey + " not found in: " + schema);
    }
    UDFContext context = UDFContext.getUDFContext();
    Properties p =
        context.getUDFProperties(this.getClass(), new String[] { udfcSignature });
    p.setProperty(DOWNLOADER_FIELD_KEY_INDEX_PROPERTY, String.valueOf(splitFieldIndex));
  }

  @Override
  public void setStoreFuncUDFContextSignature(String signature) {
    udfcSignature = signature;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void prepareToWrite(RecordWriter writer) throws IOException {
    UDFContext context = UDFContext.getUDFContext();
    Properties prop =
        context.getUDFProperties(this.getClass(), new String[] { udfcSignature });
    this.splitFieldIndex =
        Integer.valueOf(prop.getProperty(DOWNLOADER_FIELD_KEY_INDEX_PROPERTY, String.valueOf(splitFieldIndex)));
    if (splitFieldIndex < 0) {
      RuntimeException e = new RuntimeException("Key index is not set: " + splitFieldIndex);
      log.error("Failed", e);
      throw e;
    }
    this.writer = writer;
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    job.getConfiguration().set("mapred.textoutputformat.separator", "");
    FileOutputFormat.setOutputPath(job, new Path(location));
    if (comp == Compression.bz2 || comp == Compression.bz) {
      FileOutputFormat.setCompressOutput(job, true);
      FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
    } else if (comp == Compression.gz) {
      job.getConfiguration().set("zlib.compress.level", "BEST_SPEED");
      FileOutputFormat.setCompressOutput(job, true);
      FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    }
  }

  // --------------------------------------------------------------------------
  // Implementation of OutputFormat

  public static class MultiStorageOutputFormat extends
      TextOutputFormat<String, Tuple> {

    /**
     * 
     */
    private static final int DEFAULT_BUFFER_SIZE = 131072;
    private String keyValueSeparator = "\\t";
    private byte fieldDel = '\t';

    @Override
    public RecordWriter<String, Tuple>
        getRecordWriter(TaskAttemptContext context
        ) throws IOException, InterruptedException {

      final TaskAttemptContext ctx = context;

      return new RecordWriter<String, Tuple>() {

        private Map<String, MyLineRecordWriter> storeMap =
            new HashMap<String, MyLineRecordWriter>();

        private static final int BUFFER_SIZE = 1024;

        private ByteArrayOutputStream mOut =
            new ByteArrayOutputStream(BUFFER_SIZE);

        @Override
        public void write(String key, Tuple val) throws IOException {
          int sz = val.size();
          for (int i = 0; i < sz; i++) {
            Object field;
            try {
              field = val.get(i);
            } catch (ExecException ee) {
              throw ee;
            }

            StorageUtil.putField(mOut, field);

            if (i != sz - 1) {
              mOut.write(fieldDel);
            }
          }

          getStore(key).write(null, new Text(mOut.toByteArray()));

          mOut.reset();
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException {
          for (MyLineRecordWriter out : storeMap.values()) {
            out.close(context);
          }
        }

        private MyLineRecordWriter getStore(String fieldValue) throws IOException {
          MyLineRecordWriter store = storeMap.get(fieldValue);
          if (store == null) {
            DataOutputStream os = createOutputStream(fieldValue);
            store = new MyLineRecordWriter(os, keyValueSeparator);
            storeMap.put(fieldValue, store);
          }
          return store;
        }

        private DataOutputStream createOutputStream(String fieldValue) throws IOException {
          Configuration conf = ctx.getConfiguration();
          TaskID taskId = ctx.getTaskAttemptID().getTaskID();

          // Check whether compression is enabled, if so get the extension and add them to the path
          boolean isCompressed = getCompressOutput(ctx);
          CompressionCodec codec = null;
          String extension = "";
          if (isCompressed) {
            Class<? extends CompressionCodec> codecClass =
                getOutputCompressorClass(ctx, GzipCodec.class);
            codec = ReflectionUtils.newInstance(codecClass, ctx.getConfiguration());
            extension = codec.getDefaultExtension();
          }

          NumberFormat nf = NumberFormat.getInstance();
          nf.setMinimumIntegerDigits(4);

          Path path = new Path(PREFIX + fieldValue + extension, fieldValue + '-'
              + nf.format(taskId.getId()) + extension);
          Path workOutputPath = ((FileOutputCommitter) getOutputCommitter(ctx)).getWorkPath();
          Path file = new Path(workOutputPath, path);
          FileSystem fs = file.getFileSystem(conf);
          FSDataOutputStream fileOut = fs.create(file, false);

          if (isCompressed) {
            return new DataOutputStream(
                new BufferedOutputStream(codec.createOutputStream(fileOut), DEFAULT_BUFFER_SIZE));
          } else {
            return new DataOutputStream(new BufferedOutputStream(fileOut, DEFAULT_BUFFER_SIZE));
          }
        }

      };
    }

    public void setKeyValueSeparator(String sep) {
      keyValueSeparator = sep;
      fieldDel = StorageUtil.parseFieldDel(keyValueSeparator);
    }

    // ------------------------------------------------------------------------
    //

    @SuppressWarnings("rawtypes")
    protected static class MyLineRecordWriter
        extends TextOutputFormat.LineRecordWriter<WritableComparable, Text> {

      public MyLineRecordWriter(DataOutputStream out, String keyValueSeparator) {
        super(out, keyValueSeparator);
      }
    }
  }

  @Override
  public void storeSchema(ResourceSchema schema, String location, Job job) throws IOException {
    String[] fieldNames = schema.fieldNames();
    Preconditions.checkNotNull(fieldNames);

    // save the header to hdfs
    FileSystem fs = FileSystem.get(job.getConfiguration());
    FSDataOutputStream headerWriter = fs.create(new Path(outputPath.getParent(), dataTypeLongName + HEADER));
    @Cleanup
    OutputStream os = headerWriter;
    if (comp == Compression.bz2 || comp == Compression.bz) {
      throw new RuntimeException("Unsupported format");
    } else if (comp == Compression.gz) {

      os = new GZIPOutputStream(headerWriter) {

        {
          def.setLevel(Deflater.BEST_SPEED);
        }
      };
    }
    IOUtils.write(StringUtils.join(fieldNames, "\t"), os);
    os.write(Bytes.toBytes("\n"));
  }

  @Override
  public void storeStatistics(ResourceStatistics arg0, String arg1, Job arg2) throws IOException {
    // NO IMPLEMENTATION
  }

  public static void concatenate(String dataTypeLongName, String inputDir, String outputDir, String extension)
      throws IOException {
    Path inputPath = new Path(inputDir);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    log.info("Concatenating from : {}", inputDir);
    FSDataInputStream headerReader = fs.open(new Path(inputPath.getParent(), dataTypeLongName + HEADER));
    ByteArrayOutputStream hos = new ByteArrayOutputStream(512000);
    IOUtils.copy(headerReader, hos);
    byte[] header = hos.toByteArray();

    FileStatus[] dirStatus = fs.listStatus(inputPath);
    for (val file : dirStatus) {
      log.info("File found : {}", file.getPath());
      if (file.isDirectory()) {
        log.info("Directory found: {}", file.getPath());
        String dirname = file.getPath().getName();
        if (dirname.startsWith(PREFIX)) {
          String projectCode = StringUtils.removeEnd(StringUtils.strip(dirname, PREFIX), extension);
          Path projectPath = file.getPath();
          RemoteIterator<LocatedFileStatus> partItr = fs.listFiles(projectPath, false);
          if (partItr.hasNext()) {
            log.info("Creating archive for project code: {}", projectCode);
            Path outputPath = new Path(outputDir, projectCode);
            fs.mkdirs(outputPath);
            @Cleanup
            FSDataOutputStream os = fs.create(new Path(outputPath, dataTypeLongName + '.'
                + projectCode + ".tsv" + extension));

            IOUtils.write(header, os);
            while (partItr.hasNext()) {
              LocatedFileStatus part = partItr.next();
              @Cleanup
              FSDataInputStream is = fs.open(part.getPath());
              IOUtils.copy(is, os);
            }
          }
        }
      }
    }
  }
}
