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
package org.icgc.dcc.etl.db.importer.repo;

import static com.google.common.base.Stopwatch.createStarted;
import static org.apache.commons.lang.StringUtils.repeat;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.core.id.HashIdentifierClient;
import org.icgc.dcc.etl.db.importer.cli.CollectionName;
import org.icgc.dcc.etl.db.importer.core.Importer;
import org.icgc.dcc.etl.db.importer.repo.cghub.CGHubImporter;
import org.icgc.dcc.etl.db.importer.repo.pcawg.PCAWGImporter;
import org.icgc.dcc.etl.db.importer.repo.tcga.TCGAImporter;

import com.mongodb.MongoClientURI;

/**
 * Importer for the ICGC "Data Repository" feature which imports file metadata from various external data sources.
 * 
 * @see https://wiki.oicr.on.ca/display/DCCSOFT/JSON+structure+for+ICGC+data+repository
 * @see https://wiki.oicr.on.ca/display/DCCSOFT/UI+-+The+new+ICGC+DCC+data+repository+-+Simplified+version+Phase+1
 */
@Slf4j
@RequiredArgsConstructor
public class RepositoryImporter implements Importer {

  @NonNull
  private final MongoClientURI mongoUri;

  @Override
  public CollectionName getCollectionName() {
    return CollectionName.FILES;
  }

  @Override
  public void execute() {
    val watch = createStarted();

    logBanner(" Importing PCAWG");
    val projectImporter = new PCAWGImporter(mongoUri, new HashIdentifierClient());
    projectImporter.execute();

    logBanner(" Importing CGHub");
    val cghubImporter = new CGHubImporter(mongoUri);
    cghubImporter.execute();

    logBanner(" Importing TCGA");
    val tcgaImporter = new TCGAImporter(mongoUri);
    tcgaImporter.execute();

    log.info("Finished importing repository in {}", watch);
  }

  private static void logBanner(String message) {
    log.info(banner());
    log.info(message);
    log.info(banner());
  }

  private static String banner() {
    return repeat("-", 80);
  }

}
