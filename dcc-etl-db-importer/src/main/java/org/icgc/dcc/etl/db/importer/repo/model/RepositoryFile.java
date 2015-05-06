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
package org.icgc.dcc.etl.db.importer.repo.model;

import static com.beust.jcommander.internal.Lists.newArrayList;

import java.util.List;

import lombok.Data;

@Data
public class RepositoryFile {

  String study;
  String access;

  String dataType;
  String dataSubType;
  String dataFormat;

  String experimentalStrategy;

  Repository repository = new Repository();
  RepositoryDonor donor = new RepositoryDonor();

  @Data
  public static class Repository {

    String repoType;
    String repoOrg;
    String repoEntityId;

    List<RepositoryServer> repoServer = newArrayList(new RepositoryServer());
    String repoPath;

    String fileName;
    Long fileSize;
    String fileMd5sum;
    String lastModified;

  }

  @Data
  public static class RepositoryServer {

    String repoName;
    String repoCountry;
    String repoBaseUrl;

  }

  @Data
  public static class RepositoryDonor {

    String projectCode;
    String program;
    String study;
    String primarySite;

    String donorId;
    String specimenId;
    String sampleId;

    String submittedDonorId;
    String submittedSpecimenId;
    String submittedSampleId;

    String tcgaParticipantBarcode;
    String tcgaSampleBarcode;
    String tcgaAliquotBarcode;

  }

}
