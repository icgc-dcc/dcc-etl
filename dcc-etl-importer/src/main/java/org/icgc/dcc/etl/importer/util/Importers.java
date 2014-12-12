/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.importer.util;

import static java.lang.String.format;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.URIs.getUri;

import java.net.URI;

import lombok.NoArgsConstructor;
import lombok.NonNull;

import com.mongodb.MongoClientURI;

@NoArgsConstructor(access = PRIVATE)
public final class Importers {

  private static final int DEFAULT_MONGO_PORT = 27017;
  private static final String MONGO_URI_TEMPLATE = "mongodb://localhost:%d/%s";

  /**
   * Filenames.
   */

  private static final String REACTOME_PATHWAY_HIER_FILENAME = "pathway_hierarchy.txt";
  private static final String REACTOME_PATHWAY_SUMMATION_FILENAME = "pathway_2_summation.txt";
  private static final String REACTOME_UNIPROT_FILENAME = "uniprot_2_reactome.txt";
  private static final String REACTOME_GENES_BSON_FILENAME = "genes.bson";
  private static final String CGS_FILENAME = "cancer_gene_census.tsv";

  /**
   * Artifact Constants.
   */
  private static final String IMPORT_ARTIFACT_ID = "dcc-heliotrope";
  private static final String IMPORT_ARTIFACT_GROUP_ID = "org/icgc/dcc";
  private static final String IMPORT_ARTIFACT_VERSION = "11";
  private static final String IMPORT_ARTIFACT_TYPE = "jar";
  private static final String IMPORT_ARTIFACT_PATH = "" +
      IMPORT_ARTIFACT_GROUP_ID + "/" + IMPORT_ARTIFACT_ID + "/" + IMPORT_ARTIFACT_VERSION + "/" +
      IMPORT_ARTIFACT_ID + "-" + IMPORT_ARTIFACT_VERSION + "." + IMPORT_ARTIFACT_TYPE;

  /**
   * Remote.
   */
  private static final String IMPORT_ARTIFACT_REMOTE_REPO =
      "http://seqwaremaven.oicr.on.ca/artifactory/dcc-dependencies";
  private static final String IMPORT_ARTIFACT_REMOTE_URL = IMPORT_ARTIFACT_REMOTE_REPO + "/" + IMPORT_ARTIFACT_PATH;

  /**
   * Local.
   */
  private static final String IMPORT_ARTIFACT_LOCAL_REPO = System.getProperty("user.home") + "/" + ".m2/repository";
  private static final String IMPORT_ARTIFACT_LOCAL_URL = "file:" + IMPORT_ARTIFACT_LOCAL_REPO + "/"
      + IMPORT_ARTIFACT_PATH;

  /**
   * Helpers.
   */
  private static URI getRemoteImportFileUri(@NonNull String fileName) {
    return getJarFileUri(IMPORT_ARTIFACT_REMOTE_URL, fileName);
  }

  private static URI getLocalImportFileUri(@NonNull String fileName) {
    return getJarFileUri(IMPORT_ARTIFACT_LOCAL_URL, fileName);
  }

  private static URI getJarFileUri(String path, String fileName) {
    return getUri("jar:" + path + "!" + "/" + fileName);
  }

  /**
   * Remote resource URIs.
   */
  public static final URI getRemoteReactomeUniprotUri() {
    return getRemoteImportFileUri(REACTOME_UNIPROT_FILENAME);
  }

  public static final URI getRemoteReactomeSummationUri() {
    return getRemoteImportFileUri(REACTOME_PATHWAY_SUMMATION_FILENAME);
  }

  public static final URI getRemoteReactomeHierarchyUri() {
    return getRemoteImportFileUri(REACTOME_PATHWAY_HIER_FILENAME);
  }

  public static final URI getRemoteGenesBsonUri() {
    return getRemoteImportFileUri(REACTOME_GENES_BSON_FILENAME);
  }

  public static final URI getRemoteCgsUri() {
    return getRemoteImportFileUri(CGS_FILENAME);
  }

  /**
   * Local resource URIs.
   */

  public static final URI getLocalReactomeUniprotUri() {
    return getLocalImportFileUri(REACTOME_UNIPROT_FILENAME);
  }

  public static final URI getLocalReactomeSummationUri() {
    return getLocalImportFileUri(REACTOME_PATHWAY_SUMMATION_FILENAME);
  }

  public static final URI getLocalReactomeHierarchyUri() {
    return getLocalImportFileUri(REACTOME_PATHWAY_HIER_FILENAME);
  }

  public static final URI getLocalGenesBsonUri() {
    return getLocalImportFileUri(REACTOME_GENES_BSON_FILENAME);
  }

  public static final URI getLocalCgsUri() {
    return getLocalImportFileUri(CGS_FILENAME);
  }

  public static final MongoClientURI getLocalMongoClientUri(String db) {
    return new MongoClientURI(format(MONGO_URI_TEMPLATE, DEFAULT_MONGO_PORT, db));
  }

  public static final MongoClientURI getLocalMongoClientUri(int port, String db) {
    return new MongoClientURI(format(MONGO_URI_TEMPLATE, port, db));
  }

}
