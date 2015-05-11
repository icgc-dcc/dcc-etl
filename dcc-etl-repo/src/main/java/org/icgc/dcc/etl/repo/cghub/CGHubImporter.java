/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.repo.cghub;

import static com.google.common.collect.Iterables.isEmpty;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.etl.repo.model.RepositorySource.CGHUB;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.repo.cghub.core.CGHubAnalysisDetailProcessor;
import org.icgc.dcc.etl.repo.cghub.reader.CGHubAnalysisDetailReader;
import org.icgc.dcc.etl.repo.core.RepositoryFileContext;
import org.icgc.dcc.etl.repo.core.RepositorySourceFileImporter;
import org.icgc.dcc.etl.repo.model.RepositoryFile;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * @see https://tcga-data.nci.nih.gov/datareports/codeTablesReport.htm
 */
@Slf4j
public class CGHubImporter extends RepositorySourceFileImporter {

  public CGHubImporter(RepositoryFileContext context) {
    super(CGHUB, context);
  }

  @SneakyThrows
  public void execute() {
    log.info("Reading files...");
    val details = readDetails();
    log.info("Finished reading files");

    log.info("Processing details...");
    val cghubFiles = processDetails(details);
    log.info("Finished processing details");

    if (isEmpty(cghubFiles)) {
      log.error("**** Files are emtpy! Reusing previous imported files");
      return;
    }

    log.info("Writing files...");
    writeFiles(cghubFiles, CGHUB);
    log.info("Wrote {} files", formatCount(cghubFiles));
  }

  private Iterable<ObjectNode> readDetails() {
    return new CGHubAnalysisDetailReader().readDetails();
  }

  private Iterable<RepositoryFile> processDetails(Iterable<ObjectNode> details) {
    val processor = new CGHubAnalysisDetailProcessor(context);
    return processor.processDetails(details);
  }

}
