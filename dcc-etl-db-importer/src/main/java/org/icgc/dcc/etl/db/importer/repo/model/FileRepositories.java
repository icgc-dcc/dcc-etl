/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
 *                                                                                                               
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with                                  
 * this program. If not, see <http://www.gnu.org/licenses/>.                                                     
 *                                                                                                               
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS"AS IS" AND ANY                           
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES                          
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT                           
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,                                
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED                          
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;                               
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER                              
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN                         
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.etl.db.importer.repo.model;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.etl.db.importer.repo.model.FileRepositoryOrg.CGHUB;
import static org.icgc.dcc.etl.db.importer.repo.model.FileRepositoryOrg.PCAWG;
import static org.icgc.dcc.etl.db.importer.repo.model.FileRepositoryOrg.TCGA;
import static org.icgc.dcc.etl.db.importer.repo.model.FileRepositoryType.GNOS;
import static org.icgc.dcc.etl.db.importer.repo.model.FileRepositoryType.WEB_ARCHIVE;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import lombok.val;

import org.icgc.dcc.etl.db.importer.repo.model.FileRepositories.RepositoryServer.RepositoryServerBuilder;

import com.google.common.collect.ImmutableList;

@NoArgsConstructor(access = PRIVATE)
public final class FileRepositories {

  /**
   * Constants.
   */
  public static final String FILE_REPOSITORY_TYPE_FIELD_NAME = "repository.repo_type";

  // @formatter:off
  public static final List<RepositoryServer> SERVERS = ImmutableList.of(
      server().org(CGHUB).type(GNOS).name("CGHub - Santa Cruz").country("US").baseUrl("https://cghub.ucsc.edu/").build(),
      server().org(TCGA).type(WEB_ARCHIVE).name("TCGA DCC - Washington").country("US").baseUrl("https://tcga-data.nci.nih.gov/").build(),
      server().org(PCAWG).type(GNOS).name("PCAWG - Barcelona").country("ES").baseUrl("https://gtrepo-bsc.annailabs.com/").build(),
      server().org(PCAWG).type(GNOS).name("PCAWG - Santa Cruz").country("US").baseUrl("https://cghub.ucsc.edu/").build(),
      server().org(PCAWG).type(GNOS).name("PCAWG - Tokyo").country("JP").baseUrl("https://gtrepo-riken.annailabs.com/").build(),
      server().org(PCAWG).type(GNOS).name("PCAWG - Seoul").country("KR").baseUrl("https://gtrepo-etri.annailabs.com/").build(),
      server().org(PCAWG).type(GNOS).name("PCAWG - London").country("UK").baseUrl("https://gtrepo-ebi.annailabs.com/").build(),
      server().org(PCAWG).type(GNOS).name("PCAWG - Heidelberg").country("DE").baseUrl("https://gtrepo-dkfz.annailabs.com/").build(),
      server().org(PCAWG).type(GNOS).name("PCAWG - Chicago (ICGC)").country("US").baseUrl("https://gtrepo-osdc-icgc.annailabs.com/").build(),
      server().org(PCAWG).type(GNOS).name("PCAWG - Chicago (TCGA)").country("US").baseUrl("https://gtrepo-osdc-tcga.annailabs.com/").build()
      );
  // @formatter:on

  @NonNull
  public static Iterable<RepositoryServer> getServers() {
    return SERVERS;
  }

  public static RepositoryServer getCGHubServer() {
    for (val server : getServers()) {
      if (server.getOrg() == CGHUB) {
        return server;
      }
    }

    return null;
  }

  public static RepositoryServer getTCGAServer() {
    for (val server : getServers()) {
      if (server.getOrg() == TCGA) {
        return server;
      }
    }

    return null;
  }

  @NonNull
  public static RepositoryServer getPCAWGServer(String genosRepo) {
    for (val server : getServers()) {
      if (server.getOrg() == PCAWG && server.getBaseUrl().equals(genosRepo)) {
        return server;
      }
    }

    return null;
  }

  @NonNull
  public static final String formatDateTime(LocalDateTime dateTime) {
    return dateTime.format(DateTimeFormatter.ISO_INSTANT);
  }

  @NonNull
  public static final String formatDateTime(ZonedDateTime dateTime) {
    return dateTime.format(DateTimeFormatter.ISO_INSTANT);
  }

  @NonNull
  public static final String formatDateTime(Instant dateTime) {
    return dateTime.toString();
  }

  @Value
  @Builder
  public static class RepositoryServer {

    FileRepositoryType type;
    FileRepositoryOrg org;
    String name;
    String location;
    String country;
    String baseUrl;

  }

  private static RepositoryServerBuilder server() {
    return RepositoryServer.builder();
  }

}
