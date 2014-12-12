package org.icgc.dcc.etl.exporter.pig.udf;

import static com.google.common.base.Preconditions.checkState;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.StorageUtil;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.tools.pigstats.PigStatusReporter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

@Slf4j
public class DataDownloadStorage extends StoreFunc {

  private final Path outputRootPath; // User specified output Path
  private Path outputTypePath; // User specified output Path
  private int splitFieldIndex = -1; // Index of the key field
  private final String fieldDel; // delimiter of the output record.
  private Compression comp; // Compression type of output data.
  private final String dataType;
  private String udfcSignature;
  private final static String EXPORT_ROOT_LOCATION_PROPERTY = "icgc.dcc.data.exporter.root.location";
  private final static String EXPORT_TYPE_LOCATION_PROPERTY = "icgc.dcc.data.exporter.access.location";
  private final static String DATA_TYPE_PROPERTY = "icgc.dcc.data.exporter.data.type";

  private static final int BUFFER_SIZE = 65536;

  // Compression types supported by this store
  enum Compression {
    none, bz2, bz, gz;
  };

  public DataDownloadStorage(String parentPathStr, String childPathStr,
      String dataType, String splitFieldIndex) {
    this(parentPathStr, childPathStr, dataType, splitFieldIndex,
        "none", "\\t");
  }

  public DataDownloadStorage(String parentPathStr, String childPathStr,
      String dataType, String splitFieldIndex,
      String compression) {

    this(parentPathStr, childPathStr, dataType, splitFieldIndex,
        compression, "\\t");
  }

  /**
   * Constructor
   * 
   * @param parentPathStr Parent output dir path
   * @param splitFieldIndex key field index
   * @param compression 'bz2', 'bz', 'gz' or 'none'
   * @param fieldDel Output record field delimiter.
   */
  public DataDownloadStorage(String parentPathStr, String childPathStr,
      String dataType, String splitFieldIndex,
      String compression, String fieldDel) {
    this.outputRootPath = new Path(parentPathStr);

    if (!childPathStr.equals("")) {
      this.outputTypePath = new Path(childPathStr);
    }

    this.dataType = dataType;
    this.splitFieldIndex = Integer.parseInt(splitFieldIndex);
    this.fieldDel = fieldDel;
    try {
      this.comp = (compression == null) ? Compression.none : Compression
          .valueOf(compression.toLowerCase());
    } catch (IllegalArgumentException e) {
      System.err.println("Exception when converting compression string: "
          + compression + " to enum. No compression will be used");
      this.comp = Compression.none;
    }
  }

  public DataDownloadStorage(String parentPathStr, String childPathStr,
      String dataType, String splitFieldIndex,
      String compression, String fieldDel, String schema) {

    this.outputRootPath = new Path(parentPathStr);

    if (!childPathStr.equals("")) {
      this.outputTypePath = new Path(childPathStr);
    }

    this.dataType = dataType;
    this.splitFieldIndex = Integer.parseInt(splitFieldIndex);
    this.fieldDel = fieldDel;
    this.schema = schema.split("\\s*,\\s*");
    try {
      this.comp = (compression == null) ? Compression.none : Compression
          .valueOf(compression.toLowerCase());
    } catch (IllegalArgumentException e) {
      System.err.println("Exception when converting compression string: "
          + compression + " to enum. No compression will be used");
      this.comp = Compression.none;
    }
  }

  // Implementation of StoreFunc
  private RecordWriter<String, Tuple> writer;
  private String[] schema;

  @Override
  public void setStoreFuncUDFContextSignature(String signature) {
    udfcSignature = signature;
  }

  @Override
  public void checkSchema(ResourceSchema s) throws IOException {
    UDFContext udfc = UDFContext.getUDFContext();
    Properties p =
        udfc.getUDFProperties(this.getClass(), new String[] { udfcSignature });
    ResourceFieldSchema[] rootFields = s.getFields();
    Preconditions.checkArgument(rootFields.length == 2);
    ResourceFieldSchema[] bagFields = rootFields[1].getSchema().getFields();
    Preconditions.checkArgument(bagFields.length == 1);
    String[] fieldNames = bagFields[0].getSchema().fieldNames();
    p.setProperty("pig.datadownloadstorage.schema",
        StringUtils.join(fieldNames, ","));
  }

  @Override
  public void putNext(Tuple tuple) throws IOException {
    if (tuple.size() <= splitFieldIndex) {
      throw new IOException("split field index:" + this.splitFieldIndex
          + " >= tuple size:" + tuple.size());
    }
    String field = null;
    try {
      Object group = tuple.get(splitFieldIndex);
      switch (DataType.findType(group)) {
      case DataType.TUPLE:
        Tuple fields = DataType.toTuple(group);
        // Temporary solution for project code
        checkState(fields.size() == 2, "Not supported");
        String project = (String) fields.get(0);
        project = project.replaceAll(Pattern.quote("."), "_");
        String donor = (String) fields.get(1);
        field = project + "." + donor;
        break;
      default:
        field = String.valueOf(group);
      }

    } catch (ExecException exec) {
      throw new IOException(exec);
    }

    Tuple headerTuple = TupleFactory.getInstance().newTuple(
        Lists.newArrayList(schema));
    try {
      writer.write(String.valueOf(field), headerTuple);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    try {
      DataBag bag = (DataBag) tuple.get(1);
      Iterator<Tuple> it = bag.iterator();
      int i = 0;
      log.info("new bag of tuples...");
      while (it.hasNext()) {
        Tuple subTuple = it.next();
        writer.write(field, subTuple);

        // report only every 100,000 lines written
        if ((i % 100000) == 0) {
          PigStatusReporter.getInstance()
              .getCounter("DCC Static Exporter", "records for key: " + field).increment(1);
          PigStatusReporter.getInstance().progress();
        }
        ++i;

      }
      log.info("Bag (lines): {}", i);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public OutputFormat<?, ?> getOutputFormat() throws IOException {
    MultiStorageOutputFormat format = new MultiStorageOutputFormat();
    format.setKeyValueSeparator(fieldDel);
    return format;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter writer) throws IOException {
    UDFContext udfc = UDFContext.getUDFContext();
    Properties p =
        udfc.getUDFProperties(this.getClass(), new String[] { udfcSignature });
    String strSchema = p.getProperty("pig.datadownloadstorage.schema");
    // Reflection
    if (this.schema == null) this.schema = strSchema.split("\\s*,\\s*");
    this.writer = writer;
  }

  /**
   * TODO: move hadoop properties to {@link HadoopConstants}.
   */
  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    job.getConfiguration().set("mapred.textoutputformat.separator", "");
    job.getConfiguration().set(EXPORT_ROOT_LOCATION_PROPERTY,
        outputRootPath.toUri().getPath());
    if (outputTypePath != null) job.getConfiguration().set(EXPORT_TYPE_LOCATION_PROPERTY,
        outputTypePath.toUri().getPath());
    job.getConfiguration().set(DATA_TYPE_PROPERTY, dataType);
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

  // Implementation of OutputFormat
  public static class MultiStorageOutputFormat extends
      TextOutputFormat<String, Tuple> {

    private String keyValueSeparator = "\\t";
    private byte fieldDel = '\t';

    @Override
    public RecordWriter<String, Tuple> getRecordWriter(
        TaskAttemptContext context) throws IOException,
        InterruptedException {

      final TaskAttemptContext ctx = context;

      return new RecordWriter<String, Tuple>() {

        private final Map<String, MyLineRecordWriter> storeMap = new HashMap<String, MyLineRecordWriter>();

        private final ByteArrayOutputStream mOut = new ByteArrayOutputStream(
            BUFFER_SIZE);

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

            if (i != (sz - 1)) {
              mOut.write(fieldDel);
            }
          }

          getStore(key).write(null, new Text(mOut.toByteArray()));

          mOut.reset();
        }

        @Override
        public void close(TaskAttemptContext context)
            throws IOException {
          for (MyLineRecordWriter out : storeMap.values()) {
            out.close(context);
          }
        }

        private MyLineRecordWriter getStore(String fieldValue)
            throws IOException {
          MyLineRecordWriter store = storeMap.get(fieldValue);
          if (store == null) {
            DataOutputStream os = createOutputStream(fieldValue);
            store = new MyLineRecordWriter(os, keyValueSeparator);
            storeMap.put(fieldValue, store);
          }
          return store;
        }

        private DataOutputStream createOutputStream(String projectId)
            throws IOException {
          Configuration conf = ctx.getConfiguration();
          // TaskID taskId = ctx.getTaskAttemptID().getTaskID();

          // Check whether compression is enabled, if so get the
          // extension and add them to the path
          boolean isCompressed = getCompressOutput(ctx);
          CompressionCodec codec = null;
          String extension = "";
          if (isCompressed) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
                ctx, GzipCodec.class);
            codec = ReflectionUtils.newInstance(
                codecClass, ctx.getConfiguration());
            extension = codec.getDefaultExtension();
          }

          // NumberFormat nf = NumberFormat.getInstance();
          // nf.setMinimumIntegerDigits(4);

          Path workOutputPath = ((FileOutputCommitter) getOutputCommitter(ctx))
              .getWorkPath();
          String exportRootLocation = conf.get(
              EXPORT_ROOT_LOCATION_PROPERTY, workOutputPath
                  .toUri().getPath());
          String exportTypeLocation = conf.get(
              EXPORT_TYPE_LOCATION_PROPERTY, null);
          Path typePath = null;
          String dataType = conf.get(DATA_TYPE_PROPERTY, "unknown");

          Path filePath = null;
          if (exportTypeLocation != null) {
            // Static format
            typePath = new Path(exportRootLocation, projectId);
            filePath = new Path(typePath, dataType + '.'
                + projectId + ".tsv" + extension);
          } else {
            // Dynamic format
            typePath = new Path(exportRootLocation);
            filePath = new Path(typePath, projectId + '.'
                + dataType + ".tsv" + extension);
          }

          Path file = new Path(exportRootLocation, filePath);
          FileSystem fs = file.getFileSystem(conf);
          // overwrite if already there
          // FSDataOutputStream fileOut = fs.create(file, true);
          log.info("Starting a new stream...");
          FSDataOutputStream fileOut = fs.create(file, true);

          if (isCompressed) {
            return new DataOutputStream(new BufferedOutputStream(codec.createOutputStream(fileOut), BUFFER_SIZE));
          } else {
            return new DataOutputStream(new BufferedOutputStream(fileOut));
          }
        }

      };
    }

    public void setKeyValueSeparator(String sep) {
      keyValueSeparator = sep;
      fieldDel = StorageUtil.parseFieldDel(keyValueSeparator);
    }

    @SuppressWarnings("rawtypes")
    protected static class MyLineRecordWriter extends
        TextOutputFormat.LineRecordWriter<WritableComparable, Text> {

      public MyLineRecordWriter(DataOutputStream out,
          String keyValueSeparator) {
        super(out, keyValueSeparator);
      }
    }
  }
}
