package org.icgc.dcc.etl.db.importer.go.util;

import static org.icgc.dcc.etl.db.importer.go.reader.GoInferredTreeReader.RELATION_IDS;

import java.io.IOException;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.go.GoImporter;
import org.icgc.dcc.etl.db.importer.go.reader.GoInferredTreeReader;
import org.junit.Ignore;
import org.junit.Test;
import org.obolibrary.oboformat.parser.OBOFormatParserException;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

@Slf4j
@Ignore("This is being testing by the GoImporterTest. Useful for debugging though.")
public class GoInferredTreeBuilderTest {

  @Test
  public void testBuild() throws OWLOntologyCreationException, OBOFormatParserException, IOException {
    val graph = new GoInferredTreeReader(GoImporter.DEFAULT_OBO_URL).readGraph();
    val goId = "GO:0005794";
    val goTerm = graph.getOWLClassByIdentifier(goId);

    val inferredTreeBuilder = new GoInferredTreeBuilder(graph, RELATION_IDS);
    val inferredTree = inferredTreeBuilder.build(goTerm);
    log.info("Inferred tree: {}", inferredTree);
  }

}
