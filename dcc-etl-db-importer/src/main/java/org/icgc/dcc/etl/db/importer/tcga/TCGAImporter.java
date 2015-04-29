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
package org.icgc.dcc.etl.db.importer.tcga;

import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;

import java.io.IOException;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.tcga.core.TCGAClinicalFileProcessor;
import org.icgc.dcc.etl.db.importer.tcga.model.TCGAClinicalFile;
import org.icgc.dcc.etl.db.importer.tcga.writer.TCGAClinicalFileWriter;

import com.mongodb.MongoClientURI;

/**
 * @see http://tcga-data.nci.nih.gov/datareports/resources/latestarchive
 */
@Slf4j
@RequiredArgsConstructor
public class TCGAImporter {

  /**
   * Configuration
   */
  @NonNull
  private final MongoClientURI mongoUri;

  public void execute() throws IOException {
    log.info("Reading clinical files...");
    val clinicalFiles = readClinicalFiles();
    log.info("Read {} clinical files", formatCount(clinicalFiles));

    log.info("Writing clinical files...");
    writeClinicalFiles(clinicalFiles);
    log.info("Wrote {} clinical files", formatCount(clinicalFiles));
  }

  private Iterable<TCGAClinicalFile> readClinicalFiles() {
    return new TCGAClinicalFileProcessor().process();
  }

  private void writeClinicalFiles(Iterable<TCGAClinicalFile> clinicalFiles) throws IOException {
    @Cleanup
    val writer = new TCGAClinicalFileWriter(mongoUri);
    writer.write(clinicalFiles);
  }

}
