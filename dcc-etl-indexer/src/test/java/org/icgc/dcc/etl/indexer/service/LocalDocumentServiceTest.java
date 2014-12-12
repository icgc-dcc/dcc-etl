package org.icgc.dcc.etl.indexer.service;

import static org.icgc.dcc.etl.indexer.factory.DocumentServiceFactory.newLocalService;
import static org.icgc.dcc.etl.indexer.model.DocumentType.DONOR_CENTRIC_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.DONOR_TEXT_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.DONOR_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.GENE_CENTRIC_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.GENE_SET_TEXT_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.GENE_TEXT_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.MUTATION_CENTRIC_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.MUTATION_TEXT_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.OBSERVATION_CENTRIC_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.PROJECT_TEXT_TYPE;
import static org.icgc.dcc.etl.indexer.model.DocumentType.PROJECT_TYPE;

import java.io.IOException;

import org.icgc.dcc.etl.indexer.model.DocumentType;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class LocalDocumentServiceTest extends AbstractDocumentServiceTest {

  @Before
  public void setup() {
    this.service = newLocalService(config);
  }

  @Test
  public void testWriteDocuments() throws IOException {
    test(DocumentType.values());
  }

  @Test
  @Ignore
  public void testWriteTextDocuments() throws IOException {
    test(PROJECT_TEXT_TYPE, DONOR_TEXT_TYPE, GENE_TEXT_TYPE, MUTATION_TEXT_TYPE, GENE_SET_TEXT_TYPE);
  }

  @Test
  public void testWriteProjectDocuments() throws IOException {
    test(PROJECT_TYPE);
  }

  @Test
  @Ignore
  public void testWriteDonorDocuments() throws IOException {
    test(DONOR_TYPE);
  }

  @Test
  @Ignore
  public void testWriteDonorCentricDocuments() throws IOException {
    test(DONOR_CENTRIC_TYPE);
  }

  @Test
  @Ignore
  public void testWriteGeneCentricDocuments() throws IOException {
    test(GENE_CENTRIC_TYPE);
  }

  @Test
  @Ignore
  public void testWriteObservationCentricDocuments() throws IOException {
    test(OBSERVATION_CENTRIC_TYPE);
  }

  @Test
  public void testWriteMutationCentricDocuments() throws IOException {
    test(MUTATION_CENTRIC_TYPE);
  }

  @Test
  @Ignore
  public void testWriteDonorTextDocuments() throws IOException {
    test(DONOR_TEXT_TYPE);
  }

  @Test
  @Ignore
  public void testWriteGeneTextDocuments() throws IOException {
    test(GENE_TEXT_TYPE);
  }

  @Test
  @Ignore
  public void testWriteProjectTextDocuments() throws IOException {
    test(PROJECT_TEXT_TYPE);
  }

  @Test
  @Ignore
  public void testWriteMutationTextDocuments() throws IOException {
    test(MUTATION_TEXT_TYPE);
  }

}
