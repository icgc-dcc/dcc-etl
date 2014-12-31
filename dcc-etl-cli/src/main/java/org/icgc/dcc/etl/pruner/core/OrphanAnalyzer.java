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
package org.icgc.dcc.etl.pruner.core;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Stopwatch.createStarted;
import static com.google.common.collect.Maps.filterKeys;
import static com.google.common.collect.Maps.filterValues;
import static com.google.common.collect.Sets.filter;
import static org.icgc.dcc.etl.pruner.util.Orphans.getLineMapping;
import static org.icgc.dcc.etl.pruner.util.Orphans.getOrphans;
import static org.icgc.dcc.etl.pruner.util.Orphans.parseDonorFiles;
import static org.icgc.dcc.etl.pruner.util.Orphans.parseMetaFiles;
import static org.icgc.dcc.etl.pruner.util.Orphans.parseSampleFiles;
import static org.icgc.dcc.etl.pruner.util.Orphans.parseSpecimenFiles;

import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.FileTypes.FileSubType;
import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.common.hadoop.dcc.SubmissionInputData;
import org.icgc.dcc.etl.pruner.model.Orphan;
import org.icgc.dcc.submission.dictionary.model.Dictionary;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Identifies {@link Orphan}s in clinical files.
 */
@Slf4j
@RequiredArgsConstructor
public class OrphanAnalyzer {

  /**
   * Dependencies.
   */
  @NonNull
  private final FileSystem fileSystem;
  @NonNull
  private final Dictionary dictionary;

  public Iterable<Orphan> analyze(@NonNull String inputDirectory, @NonNull Iterable<String> projectNames) {
    val timer = createStarted();
    val orphans = ImmutableSet.<Orphan> builder();
    val matchingFiles = SubmissionInputData.getMatchingFiles(
        fileSystem,
        inputDirectory,
        Sets.newHashSet(projectNames),
        dictionary.getPatterns());

    for (val projectKey : matchingFiles.keySet()) {
      log.info("Processing Project '{}' ", projectKey);
      val fileTypeToFiles = matchingFiles.get(projectKey);
      Set<Orphan> projectOrphans = null;
      try {
        projectOrphans = processProject(fileSystem, dictionary, projectKey, fileTypeToFiles);
      } catch (IllegalStateException e) {
        log.warn("Failed Processing Project '{}', skipping... ", projectKey);
        continue;
      }
      orphans.addAll(projectOrphans);
      log.info("Finished processing Project '{}' ", projectKey);
    }

    log.info("Analyzing orphans took: {}", timer.stop());
    return orphans.build();
  }

  private static Set<Orphan> processProject(FileSystem fileSystem, Dictionary dictionary,
      String projectName, Map<FileType, List<Path>> fileTypeToFiles) {
    val orphans = ImmutableSet.<Orphan> builder();

    log.debug("Reading {}", FileType.DONOR_TYPE);
    val donorInputFiles = fileTypeToFiles.get(FileType.DONOR_TYPE);
    checkState(!donorInputFiles.isEmpty(), "donor input file is missing.");
    val donorLines = parseDonorFiles(fileSystem, dictionary, donorInputFiles);
    val donorIds = donorLines.keySet();
    log.debug("Finished reading {}", FileType.DONOR_TYPE);

    log.debug("Reading {}", FileType.SPECIMEN_TYPE);
    val specimenInputFiles = fileTypeToFiles.get(FileType.SPECIMEN_TYPE);
    checkState(!specimenInputFiles.isEmpty(), "specimen input file is missing.");
    val specimenLines = parseSpecimenFiles(fileSystem, dictionary, specimenInputFiles);
    val specimenIdToDonorIdMapping = getLineMapping(specimenLines);
    log.debug("Finished reading {}", FileType.SPECIMEN_TYPE);

    log.debug("Reading {}", FileType.SAMPLE_TYPE);
    val sampleInputFiles = fileTypeToFiles.get(FileType.SAMPLE_TYPE);
    checkState(!sampleInputFiles.isEmpty(), "sample input file is missing.");
    val sampleLines = parseSampleFiles(fileSystem, dictionary, sampleInputFiles);
    val sampleIdToSpecimenIdMapping = getLineMapping(sampleLines);
    log.debug("Finished reading {}", FileType.SAMPLE_TYPE);

    log.debug("Reading {}", FileSubType.META_SUBTYPE);
    val metaLines = parseMetaFiles(fileSystem, dictionary, fileTypeToFiles);
    val metaSampleIds = metaLines.keySet();
    log.debug("Finished reading {}", FileSubType.META_SUBTYPE);

    log.debug("Finding valid ids");
    val validSampleIdToSpecimenIdMappings =
        ImmutableMap.copyOf(filterKeys(sampleIdToSpecimenIdMapping, in(metaSampleIds)));
    val validSpecimenIds = Sets.<String> newHashSet(validSampleIdToSpecimenIdMappings.values());
    val validSpecimenIdToDonorIdMappings =
        ImmutableMap.copyOf(filterKeys(specimenIdToDonorIdMapping, in(validSpecimenIds)));
    val validDonorIdsSet = Sets.<String> newHashSet(validSpecimenIdToDonorIdMappings.values());
    log.debug("Finished finding valid ids");

    val orphanedDonorIds = ImmutableSet.copyOf(filter(donorIds, not(in(validDonorIdsSet))));
    log.info("Total donors: {}, total valid: {}, total orphans: {}", Iterables.size(donorIds),
        Iterables.size(validDonorIdsSet), Iterables.size(orphanedDonorIds));

    val orphanedSpecimenIds =
        ImmutableMap.copyOf(filterValues(specimenIdToDonorIdMapping, in(orphanedDonorIds))).keySet();
    log.info("Total Specimen: {}, total valid: {}, total orphans: {}",
        Iterables.size(specimenIdToDonorIdMapping.keySet()),
        Iterables.size(specimenIdToDonorIdMapping.keySet()) - Iterables.size(orphanedSpecimenIds),
        Iterables.size(orphanedSpecimenIds));

    val orphanedSampleIds =
        ImmutableMap.copyOf(filterValues(sampleIdToSpecimenIdMapping, in(orphanedSpecimenIds))).keySet();
    log.info("Total Samples: {}, total valid: {}, total orphans: {}",
        Iterables.size(sampleIdToSpecimenIdMapping.keySet()),
        Iterables.size(sampleIdToSpecimenIdMapping.keySet()) - Iterables.size(orphanedSampleIds),
        Iterables.size(orphanedSampleIds));

    val donorOrphans = getOrphans(projectName, FileType.DONOR_TYPE, donorLines, orphanedDonorIds);
    val specimenOrphans = getOrphans(projectName, FileType.SPECIMEN_TYPE, specimenLines, orphanedSpecimenIds);
    val sampleOrphans = getOrphans(projectName, FileType.SAMPLE_TYPE, sampleLines, orphanedSampleIds);

    orphans
        .addAll(donorOrphans)
        .addAll(specimenOrphans)
        .addAll(sampleOrphans);

    return orphans.build();
  }
}
