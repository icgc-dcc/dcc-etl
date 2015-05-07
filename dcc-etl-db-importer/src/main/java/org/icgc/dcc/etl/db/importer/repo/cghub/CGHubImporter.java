/*
 * Copyright (c) 2013 The Ontario Institute for Cancer Research. All rights reserved.                             
 *                                                                                                               
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with                                  
 * this program. If not, see <http://www.gnu.org/licenses/>.                                                     
 *                                                                                                               
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY                           
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES                          
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT                           
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,                                
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED                          
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;                               
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER                              
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN                         
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.etl.db.importer.repo.cghub;

import static com.google.common.base.Stopwatch.createStarted;
import static org.icgc.dcc.etl.db.importer.repo.cghub.util.CGHubProjects.getProjects;

import java.util.Map;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.tcga.TCGAClient;
import org.icgc.dcc.etl.core.id.IdentifierClient;
import org.icgc.dcc.etl.db.importer.repo.RespositoryTypeImporter;
import org.icgc.dcc.etl.db.importer.repo.cghub.core.CGHubAnalysisDetailProcessor;
import org.icgc.dcc.etl.db.importer.repo.cghub.reader.CGHubAnalysisDetailReader;
import org.icgc.dcc.etl.db.importer.repo.cghub.writer.CGHubFileWriter;

import com.mongodb.MongoClientURI;

/**
 * @see https://tcga-data.nci.nih.gov/datareports/codeTablesReport.htm
 */
@Slf4j
public class CGHubImporter extends RespositoryTypeImporter {

  public CGHubImporter(MongoClientURI mongoUri, Map<String, String> primarySites, IdentifierClient identifierClient,
      TCGAClient tcgaClient) {
    super(mongoUri, primarySites, identifierClient, tcgaClient);
  }

  @SneakyThrows
  public void execute() {
    log.info("Starting import...");
    val watch = createStarted();

    val reader = new CGHubAnalysisDetailReader();
    val processor = new CGHubAnalysisDetailProcessor(primarySites, identifierClient, tcgaClient);
    @Cleanup
    val writer = new CGHubFileWriter(mongoUri);

    writer.clearFiles();
    try {
      for (val diseaseCode : getProjects()) {
        log.info("Reading project details '{}'...", diseaseCode);
        val details = reader.readDetails(diseaseCode);

        val cghubFiles = processor.processDetails(diseaseCode, details);
        for (val cghubFile : cghubFiles) {
          writer.write(cghubFile);
        }

        log.info("Finished importing project '{}'.", diseaseCode);
      }
    } finally {
      log.info("Finished importing records in {}.", watch.stop());
    }
  }

}
