package org.icgc.dcc.etl.loader.mongodb;

import cascading.tuple.TupleEntry;

import com.mongodb.DBObject;

public interface TupleEntryToDBObjectTransformer {

  /**
   * Transform a {@link TupleEntry} into a {@link DBObject} that can be saved in mongodb.
   */
  DBObject transformEntry(TupleEntry entry);

}
