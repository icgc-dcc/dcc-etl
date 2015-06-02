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
package org.icgc.dcc.etl.summarizer.core;

import static org.icgc.dcc.common.core.model.FieldNames.MONGO_INTERNAL_ID;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_COMPLETE_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_DATE;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_MUTATED_GENE_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_NAME;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_NUMBER;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_PRIMARY_SITE_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_PROJECT_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_SAMPLE_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_SPECIMEN_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_SSM_COUNT;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.summarizer.repository.ReleaseRepository;
import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.primitives.Ints;

@Slf4j
public class ReleaseSummarizer extends AbstractSummarizer {

  private final String releaseId;
  private final String releaseName;

  public ReleaseSummarizer(String releaseId, String releaseName, ReleaseRepository repository) {
    super(repository);
    this.releaseId = releaseId;
    this.releaseName = releaseName;
  }

  @Override
  public void summarize() {
    log.info("Finding release project count...");
    long projectCount = repository.getReleaseProjectCount();
    log.info("Found {} release projects", projectCount);

    log.info("Finding release complete project count...");
    long completeProjectCount = repository.getReleaseCompleteProjectCount();
    log.info("Found {} release complete projects", completeProjectCount);

    log.info("Finding release unique primary site count...");
    long uniquePrimarySiteCount = repository.getReleaseUniquePrimarySiteCount();
    log.info("Found {} release projects", uniquePrimarySiteCount);

    log.info("Finding release donor count...");
    long donorCount = repository.getReleaseDonorCount();
    log.info("Found {} release donors", donorCount);

    log.info("Finding release complete donor count...");
    long completeDonorCount = repository.getReleaseCompleteDonorCount();
    log.info("Found {} release complete donors", donorCount);

    log.info("Finding release specimen count...");
    long specimenCount = repository.getReleaseSpecimenCount();
    log.info("Found {} release specimen", specimenCount);

    log.info("Finding release sample count...");
    long sampleCount = repository.getReleaseSampleCount();
    log.info("Found {} release samples", sampleCount);

    log.info("Finding release unique ssm count...");
    long uniqueSsmCount = repository.getReleaseUniqueSsmCount();
    log.info("Found {} release unique ssms", uniqueSsmCount);

    log.info("Finding release unique affected gene count...");
    long uniqueAffectedGeneCount = repository.getReleaseUniqueAffectedGeneCount();
    log.info("Found {} release unique affected genes", uniqueAffectedGeneCount);

    ObjectNode release = createObject();

    // Release metadata
    release.put(MONGO_INTERNAL_ID, releaseId);
    release.put(RELEASE_ID, releaseId);
    release.put(RELEASE_NAME, releaseName);
    release.put(RELEASE_NUMBER, parseReleaseNumber(releaseName));
    release.put(RELEASE_DATE, DateTime.now().toString());

    // Project counts
    release.put(RELEASE_PROJECT_COUNT, projectCount);
    release.put("complete_project_count", completeProjectCount);
    release.put(RELEASE_PRIMARY_SITE_COUNT, uniquePrimarySiteCount);

    // Donor counts
    release.put(RELEASE_DONOR_COUNT, donorCount);
    release.put(RELEASE_COMPLETE_DONOR_COUNT, completeDonorCount);
    release.put(RELEASE_SPECIMEN_COUNT, specimenCount);
    release.put(RELEASE_SAMPLE_COUNT, sampleCount);

    // Observation counts
    release.put(RELEASE_SSM_COUNT, uniqueSsmCount);
    release.put(RELEASE_MUTATED_GENE_COUNT, uniqueAffectedGeneCount);

    repository.saveRelease(release);
  }

  private Integer parseReleaseNumber(String releaseName) {
    // TODO: Pass explicitly from CLI
    return Ints.tryParse(releaseName.replaceAll("\\D+", ""));
  }

}
