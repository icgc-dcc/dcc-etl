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
package org.icgc.dcc.etl.pruner.util;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.all;
import static com.google.common.collect.Maps.filterValues;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_ANALYZED_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_SPECIMEN_ID;
import static org.icgc.dcc.etl.pruner.util.OrphanParsers.parseFileField;
import static org.icgc.dcc.etl.pruner.util.OrphanParsers.parseFileFields;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.etl.pruner.model.Line;
import org.icgc.dcc.etl.pruner.model.Orphan;
import org.icgc.dcc.submission.dictionary.model.Dictionary;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Common utilities for dealing with {@link Orphan}s.
 */
@NoArgsConstructor(access = PRIVATE)
public final class Orphans {

  public static final String PRUNER_DIR = ".pruner";

  public static final Map<FileType, String> FILETYPE_ID_FIELD_NAMES = ImmutableMap.of(
      FileType.DONOR_TYPE, SUBMISSION_DONOR_ID,
      FileType.SPECIMEN_TYPE, SUBMISSION_SPECIMEN_ID,
      FileType.SAMPLE_TYPE, SUBMISSION_ANALYZED_SAMPLE_ID);

  public static Iterable<String> getOrphanIds(@NonNull Iterable<Orphan> orphans) {
    return Iterables.transform(orphans, new Function<Orphan, String>() {

      @Override
      public String apply(Orphan orphan) {
        return orphan.getId();
      }

    });
  }

  public static Map<String, String> getLineMapping(@NonNull Map<String, Line> lines) {
    val result = ImmutableMap.<String, String> builder();
    for (val id : lines.keySet()) {
      result.put(id, lines.get(id).getValue(0));
    }

    return result.build();
  }

  public static Map<String, Line> parseDonorFiles(@NonNull FileSystem fileSystem,
      @NonNull Dictionary dictionary,
      List<Path> donorInputFiles) {
    val lines = ImmutableMap.<String, Line> builder();
    for (val file : donorInputFiles) {
      val fileLines = parseFileField(
          fileSystem,
          FileType.DONOR_TYPE,
          FILETYPE_ID_FIELD_NAMES.get(FileType.DONOR_TYPE),
          dictionary.getFileSchema(FileType.DONOR_TYPE),
          file);
      lines.putAll(fileLines);
    }

    return lines.build();
  }

  public static Map<String, Line> parseSpecimenFiles(@NonNull FileSystem fileSystem,
      @NonNull Dictionary dictionary, List<Path> specimenInputFiles) {
    val lines = ImmutableMap.<String, Line> builder();
    for (val file : specimenInputFiles) {
      val fileLines = parseFileFields(
          fileSystem,
          FileType.SPECIMEN_TYPE,
          FILETYPE_ID_FIELD_NAMES.get(FileType.SPECIMEN_TYPE),
          FILETYPE_ID_FIELD_NAMES.get(FileType.DONOR_TYPE),
          dictionary.getFileSchema(FileType.SPECIMEN_TYPE),
          file);
      lines.putAll(fileLines);
    }

    return lines.build();
  }

  public static Map<String, Line> parseSampleFiles(@NonNull FileSystem fileSystem,
      @NonNull Dictionary dictionary,
      @NonNull List<Path> sampleInputFiles) {
    val lines = ImmutableMap.<String, Line> builder();
    for (val file : sampleInputFiles) {
      val fileLines = parseFileFields(
          fileSystem,
          FileType.SAMPLE_TYPE,
          FILETYPE_ID_FIELD_NAMES.get(FileType.SAMPLE_TYPE),
          FILETYPE_ID_FIELD_NAMES.get(FileType.SPECIMEN_TYPE),
          dictionary.getFileSchema(FileType.SAMPLE_TYPE),
          file);
      lines.putAll(fileLines);
    }

    return lines.build();
  }

  public static Map<String, Line> parseMetaFiles(@NonNull FileSystem fileSystem, @NonNull Dictionary dictionary,
      @NonNull Map<FileType, List<Path>> fileTypeToFiles) {
    val uniqueSampleIds = new HashMap<String, Line>();
    val fileTypes = fileTypeToFiles.keySet();
    val metaFileTypes = Sets.filter(fileTypes, new Predicate<FileType>() {

      @Override
      public boolean apply(FileType fileType) {
        return fileType.isMeta();
      }
    });

    boolean foundMetaFile = false;
    for (val fileType : metaFileTypes) {
      val metaInputFiles = fileTypeToFiles.get(fileType);
      if (!metaInputFiles.isEmpty()) {
        foundMetaFile = true;
        val metaInputFile = metaInputFiles.get(0);
        val analyzedSampleIds = parseFileField(
            fileSystem,
            fileType,
            SUBMISSION_ANALYZED_SAMPLE_ID,
            dictionary.getFileSchema(fileType),
            metaInputFile);

        uniqueSampleIds.putAll(analyzedSampleIds);
      }
    }
    checkState(foundMetaFile, "meta files are missing.");

    return uniqueSampleIds;
  }

  public static Iterable<Orphan> getOrphans(@NonNull String projectName, @NonNull FileType fileType,
      @NonNull Map<String, Line> lines, @NonNull Set<String> orphanedIds) {
    val orphans = ImmutableSet.<Orphan> builder();
    for (val orphanId : orphanedIds) {
      val line = lines.get(orphanId);
      val orphan = Orphan.builder()
          .projectName(projectName)
          .fileType(fileType)
          .file(line.getFile())
          .id(orphanId)
          .lineNumber(line.getLineNumber())
          .build();

      orphans.add(orphan);
    }

    return orphans.build();
  }

  public static boolean isDonorIdOrphaned(@NonNull String donorId, @NonNull Map<String, String> specimenFileMap,
      @NonNull Map<String, String> sampleFileMap, @NonNull Set<String> allAnalyzedSampleIds) {
    val specimenIds = specimenIdsForDonorId(donorId, specimenFileMap);
    val sampleIds = sampleIdsForSpecimenIds(sampleFileMap, specimenIds);
    return all(sampleIds, not(in(allAnalyzedSampleIds)));
  }

  public static Set<String> sampleIdsForSpecimenIds(Map<String, String> sampleFileMap, Set<String> specimenIds) {
    return filterValues(sampleFileMap, in(specimenIds)).keySet();
  }

  public static Set<String> specimenIdsForDonorId(String donorIdToCheck, Map<String, String> specimenFileMap) {
    return filterValues(specimenFileMap, equalTo(donorIdToCheck)).keySet();
  }

}
