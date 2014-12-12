package org.icgc.dcc.etl.summarizer.util;

import static com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT;

import org.jongo.QueryModifier;

import com.mongodb.DBCursor;

public class LongBatchQueryModifier implements QueryModifier {

  @Override
  public void modify(DBCursor cursor) {
    // Prevent time outs due to idle cursors after an inactivity period (10 minutes)
    cursor.setOptions(QUERYOPTION_NOTIMEOUT);
    cursor.batchSize(Integer.MAX_VALUE);

    // Prevent a document from being retrieved multiple times because of update during iteration
    cursor.snapshot();
  }

}