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
package org.icgc.dcc.etl.db.importer.repo.tcga.core;

import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;

import java.util.regex.Pattern;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.repo.model.FileRepositoryType;
import org.icgc.dcc.etl.db.importer.repo.tcga.reader.TCGAArchiveListReader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

@Slf4j
public class TCGAClinicalFileProcessor {

  /**
   * Constants.
   */
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern CLINICAL_ARCHIVE_NAME_PATTERN = Pattern.compile(".*_(\\w+)\\.bio\\..*");

  public Iterable<ObjectNode> process() {
    log.info("Reading archive list entries...");
    val entries = TCGAArchiveListReader.readEntries();
    log.info("Read {} archive list entries", formatCount(entries));

    val clinicalFiles = ImmutableList.<ObjectNode> builder();
    for (val entry : entries) {
      val matcher = CLINICAL_ARCHIVE_NAME_PATTERN.matcher(entry.getArchiveName());
      val clinical = matcher.matches();
      if (!clinical) {
        continue;
      }

      val diseaseCode = matcher.group(1);
      val projectName = resolveProjectName(diseaseCode);
      val archiveClinicalFiles = processArchive(projectName, entry.getArchiveUrl());

      clinicalFiles.addAll(archiveClinicalFiles);
    }

    return clinicalFiles.build();
  }

  private Iterable<ObjectNode> processArchive(String projectName, String archiveUrl) {
    val processor = new TCGAArchiveClinicalFileProcessor();
    val archiveClinicalFiles = processor.process(archiveUrl);
    log.info("Processing {} archive clinical files", formatCount(archiveClinicalFiles));

    val clinicalFiles = ImmutableList.<ObjectNode> builder();
    for (val archiveClinicalFile : archiveClinicalFiles) {
      val clinicalFile = MAPPER.createObjectNode();
      clinicalFile.putNull("study");
      clinicalFile.put("data_type", "Clinical");
      clinicalFile.put("data_subtype", "Clinical data");
      clinicalFile.putNull("experimental_strategy");
      clinicalFile.put("data_format", "XML");
      clinicalFile.put("access", "open");

      clinicalFile.putPOJO("repository_type", FileRepositoryType.TCGA);
      clinicalFile.put("project_name", projectName);
      clinicalFile.put("donor_id", archiveClinicalFile.getDonorId());
      clinicalFile.put("file_name", archiveClinicalFile.getFileName());
      clinicalFile.put("file_size", archiveClinicalFile.getFileSize());
      clinicalFile.put("file_md5", archiveClinicalFile.getFileMd5());
      clinicalFile.put("url", archiveClinicalFile.getUrl());
      clinicalFile.put("last_modified", archiveClinicalFile.getLastModified().toString());

      clinicalFiles.add(clinicalFile);
    }

    return clinicalFiles.build();
  }

  private static String resolveProjectName(String diseaseCode) {
    return diseaseCode + "-US";
  }

}
