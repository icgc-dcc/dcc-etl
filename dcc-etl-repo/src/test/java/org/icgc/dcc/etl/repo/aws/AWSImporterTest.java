package org.icgc.dcc.etl.repo.aws;

import static org.icgc.dcc.etl.repo.util.Tests.createLocalRepositoryFileContext;
import lombok.val;

import org.junit.Test;

public class AWSImporterTest {

  @Test
  // @Ignore("For development only")
  public void testExecute() {
    val context = createLocalRepositoryFileContext();
    val awsImporter = new AWSImporter(context);
    awsImporter.execute();
  }

}
