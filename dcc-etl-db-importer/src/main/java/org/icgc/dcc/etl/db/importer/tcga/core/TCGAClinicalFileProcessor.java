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
package org.icgc.dcc.etl.db.importer.tcga.core;

import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.etl.db.importer.repo.util.FileRepositories.formatDateTime;

import java.util.regex.Pattern;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.repo.model.FileRepositoryType;
import org.icgc.dcc.etl.db.importer.tcga.model.TCGAArchiveClinicalFile;
import org.icgc.dcc.etl.db.importer.tcga.model.TCGAClinicalFile;
import org.icgc.dcc.etl.db.importer.tcga.reader.TCGAArchiveListReader;
import org.icgc.dcc.etl.db.importer.tcga.reader.TCGAArchivePageReader;

import com.google.common.collect.ImmutableList;

@Slf4j
public class TCGAClinicalFileProcessor {

  /**
   * Constants.
   */
  private static final Pattern CLINICAL_ARCHIVE_NAME_PATTERN = Pattern.compile(".*_(\\w+)\\.bio\\..*");
  private static final Pattern CLINICAL_FILENAME_PATTERN = Pattern.compile(".*_clinical.([^.]+).xml");

  public Iterable<TCGAClinicalFile> process() {
    log.info("Reading archive list entries...");
    val entries = TCGAArchiveListReader.readEntries();
    log.info("Read {} archive list entries", formatCount(entries));

    val clinicalFiles = ImmutableList.<TCGAClinicalFile> builder();
    for (val entry : entries) {
      val matcher = CLINICAL_ARCHIVE_NAME_PATTERN.matcher(entry.getArchiveName());
      if (matcher.matches()) {
        val diseaseCode = matcher.group(1);
        val projectName = resolveProjectName(diseaseCode);

        for (val archiveClinicalFile : processArchiveClinicalFiles(entry.getArchiveUrl())) {
          val clinicalFile = new TCGAClinicalFile(
              FileRepositoryType.TCGA,
              projectName,
              archiveClinicalFile.getDonorId(),
              archiveClinicalFile.getFileName(),
              archiveClinicalFile.getUrl(),
              archiveClinicalFile.getLastModified().toString());

          clinicalFiles.add(clinicalFile);
        }
      }
    }

    return clinicalFiles.build();
  }

  private ImmutableList<TCGAArchiveClinicalFile> processArchiveClinicalFiles(String archiveUrl) {
    val clinicalFiles = ImmutableList.<TCGAArchiveClinicalFile> builder();

    val archiveFolderUrl = resolveArchiveFolderUrl(archiveUrl);
    for (val entry : TCGAArchivePageReader.readEntries(archiveFolderUrl)) {

      val matcher = CLINICAL_FILENAME_PATTERN.matcher(entry.getFileName());
      if (matcher.matches()) {
        val donorId = matcher.group(1);
        val url = archiveFolderUrl + "/" + entry.getFileName();
        val clinicalFile =
            new TCGAArchiveClinicalFile(donorId, entry.getFileName(), formatDateTime(entry.getLastModified()), url);

        clinicalFiles.add(clinicalFile);
      }
    }

    return clinicalFiles.build();
  }

  private static String resolveArchiveFolderUrl(String archiveUrl) {
    return archiveUrl.replaceFirst(".tar.gz$", "");
  }

  private static String resolveProjectName(String diseaseCode) {
    return diseaseCode + "-US";
  }

}
