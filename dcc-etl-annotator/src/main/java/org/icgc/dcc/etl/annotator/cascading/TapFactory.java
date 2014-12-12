package org.icgc.dcc.etl.annotator.cascading;

import static cascading.tap.SinkMode.REPLACE;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.hadoop.fs.Path;

import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.local.FileTap;

/**
 * Provider that returns a platform specific input / output {@link Tap} implementation.
 */
@RequiredArgsConstructor
public class TapFactory {

  private final boolean local;

  public Tap<?, ?, ?> createFileTap(@NonNull Path path) {
    // Force processing
    val sinkMode = REPLACE;

    // Tab delimited
    val delimiter = "\t";

    // Include header
    val hasHeader = true;

    // File
    val file = path.toUri().toString();

    if (local) {
      return new FileTap(new cascading.scheme.local.TextDelimited(hasHeader, delimiter), file, sinkMode);
    } else {
      return new Hfs(new TextDelimited(hasHeader, delimiter), file, sinkMode);
    }
  }

}
