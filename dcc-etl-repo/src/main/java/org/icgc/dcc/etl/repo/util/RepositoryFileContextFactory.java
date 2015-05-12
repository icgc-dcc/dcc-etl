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
package org.icgc.dcc.etl.repo.util;

import static lombok.AccessLevel.PRIVATE;

import java.util.Map;

import lombok.Cleanup;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

import org.icgc.dcc.common.core.tcga.TCGAClient;
import org.icgc.dcc.etl.core.id.HttpIdentifierClient;
import org.icgc.dcc.etl.core.id.IdentifierClient;
import org.icgc.dcc.etl.repo.core.RepositoryFileContext;
import org.icgc.dcc.etl.repo.core.RepositoryProjectReader;

import com.mongodb.MongoClientURI;

@NoArgsConstructor(access = PRIVATE)
public final class RepositoryFileContextFactory {

  private static final String DEFAULT_ID_SERVICE_URL = "http://hcache-dcc.oicr.on.ca:5391/";

  @NonNull
  public static RepositoryFileContext createRepositoryContext(MongoClientURI repoMongoUri, MongoClientURI geneMongoUri,
      String esUri) {
    val primarySites = getProjectPrimarySites(geneMongoUri);
    val identifierClient = createIdentifierClient(DEFAULT_ID_SERVICE_URL);
    val tcgaClient = createTCGAClient();

    return new RepositoryFileContext(repoMongoUri, esUri, primarySites, identifierClient, tcgaClient);
  }

  private static IdentifierClient createIdentifierClient(String idUrl) {
    // Not used, but needed due to reflection
    val dummyRelease = "";
    return new HttpIdentifierClient(idUrl, dummyRelease);
  }

  private static TCGAClient createTCGAClient() {
    return new TCGAClient();
  }

  @SneakyThrows
  private static Map<String, String> getProjectPrimarySites(MongoClientURI geneMongoUri) {
    @Cleanup
    val projectReader = new RepositoryProjectReader(geneMongoUri);
    return projectReader.getPrimarySites();
  }

}
