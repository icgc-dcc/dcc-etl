package org.icgc.dcc.etl.db.importer.gene;

import static org.icgc.dcc.common.core.model.ReleaseDatabase.GENOME;
import static org.icgc.dcc.etl.db.importer.util.Importers.getLocalMongoClientUri;
import static org.icgc.dcc.etl.db.importer.util.Importers.getRemoteGenesBsonUri;

import java.io.IOException;
import java.net.URISyntaxException;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.junit.Before;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

@Slf4j
public class GeneImporterTest {

  /**
   * Test configuration.
   */
  MongoClientURI releaseUri;

  /**
   * Class under test.
   */
  GeneImporter importer;

  @Before
  public void setUp() throws IOException, URISyntaxException {
    this.releaseUri = getLocalMongoClientUri("dcc-genome");
    val genesMongo = new MongoClient(releaseUri);

    log.info("Dropping '{}'...", GENOME.getId());
    genesMongo.dropDatabase(GENOME.getId());

    this.importer = new GeneImporter(releaseUri, getRemoteGenesBsonUri());
  }

  @Test
  public void testExecute() throws IOException {
    importer.execute();
  }

}
