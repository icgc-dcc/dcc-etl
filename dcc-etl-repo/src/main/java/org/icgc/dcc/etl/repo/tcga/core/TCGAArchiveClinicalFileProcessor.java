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
package org.icgc.dcc.etl.repo.tcga.core;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.repo.core.RepositoryFileProcessor;
import org.icgc.dcc.etl.repo.tcga.model.TCGAArchiveClinicalFile;
import org.icgc.dcc.etl.repo.tcga.reader.TCGAArchiveManifestReader;
import org.icgc.dcc.etl.repo.tcga.reader.TCGAArchivePageReader;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@Slf4j
public class TCGAArchiveClinicalFileProcessor {

  /**
   * Constants.
   */
  private static final Pattern CLINICAL_FILENAME_PATTERN = Pattern.compile(".*_clinical.([^.]+).xml");

  public List<TCGAArchiveClinicalFile> process(@NonNull String archiveUrl) {
    log.info("Processing archive url '{}'...", archiveUrl);
    val archiveFolderUrl = resolveArchiveFolderUrl(archiveUrl);
    val md5s = resolveArchiveFileMD5Sums(archiveFolderUrl);

    val clinicalFiles = ImmutableList.<TCGAArchiveClinicalFile> builder();
    for (val entry : TCGAArchivePageReader.readEntries(archiveFolderUrl)) {
      val matcher = CLINICAL_FILENAME_PATTERN.matcher(entry.getFileName());
      val clinical = matcher.matches();
      if (!clinical) {
        continue;
      }

      val donorId = matcher.group(1);
      val url = archiveFolderUrl + "/" + entry.getFileName();
      val md5 = md5s.get(entry.getFileName());
      val clinicalFile =
          new TCGAArchiveClinicalFile(
              donorId, entry.getFileName(), RepositoryFileProcessor.formatDateTime(entry.getLastModified()),
              entry.getFileSize(), md5, url);

      clinicalFiles.add(clinicalFile);
    }

    return clinicalFiles.build();
  }

  private Map<String, String> resolveArchiveFileMD5Sums(String archiveFolderUrl) {
    val md5s = ImmutableMap.<String, String> builder();
    val entries = TCGAArchiveManifestReader.readEntries(archiveFolderUrl);
    for (val entry : entries) {
      md5s.put(entry.getFileName(), entry.getMd5());
    }

    return md5s.build();
  }

  private static String resolveArchiveFolderUrl(String archiveUrl) {
    return archiveUrl.replaceFirst(".tar.gz$", "");
  }

}
