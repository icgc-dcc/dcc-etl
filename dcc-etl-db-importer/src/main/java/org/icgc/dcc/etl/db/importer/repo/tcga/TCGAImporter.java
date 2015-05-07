/*
j * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.db.importer.repo.tcga;

import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;

import java.util.Map;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.tcga.TCGAClient;
import org.icgc.dcc.etl.core.id.IdentifierClient;
import org.icgc.dcc.etl.db.importer.repo.RespositoryTypeImporter;
import org.icgc.dcc.etl.db.importer.repo.model.RepositoryFile;
import org.icgc.dcc.etl.db.importer.repo.tcga.core.TCGAClinicalFileProcessor;
import org.icgc.dcc.etl.db.importer.repo.tcga.writer.TCGAClinicalFileWriter;

import com.mongodb.MongoClientURI;

/**
 * @see http://tcga-data.nci.nih.gov/datareports/resources/latestarchive
 */
@Slf4j
public class TCGAImporter extends RespositoryTypeImporter {

  public TCGAImporter(MongoClientURI mongoUri, Map<String, String> primarySites, IdentifierClient identifierClient,
      TCGAClient tcgaClient) {
    super(mongoUri, primarySites, identifierClient, tcgaClient);
  }

  public void execute() {
    log.info("Reading clinical files...");
    val clinicalFiles = readClinicalFiles();
    log.info("Read {} clinical files", formatCount(clinicalFiles));

    log.info("Writing clinical files...");
    writeClinicalFiles(clinicalFiles);
    log.info("Wrote {} clinical files", formatCount(clinicalFiles));
  }

  private Iterable<RepositoryFile> readClinicalFiles() {
    return new TCGAClinicalFileProcessor(primarySites, identifierClient, tcgaClient).processClinicalFiles();
  }

  @SneakyThrows
  private void writeClinicalFiles(Iterable<RepositoryFile> clinicalFiles) {
    @Cleanup
    val writer = new TCGAClinicalFileWriter(mongoUri);
    writer.write(clinicalFiles);
  }

}
