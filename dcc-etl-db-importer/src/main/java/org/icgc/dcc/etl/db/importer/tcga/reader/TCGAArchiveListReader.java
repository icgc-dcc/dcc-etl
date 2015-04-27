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
package org.icgc.dcc.etl.db.importer.tcga.reader;

import static com.google.common.collect.Iterables.skip;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.icgc.dcc.common.core.util.Splitters.TAB;
import static org.icgc.dcc.common.core.util.URLs.getUrl;

import java.net.URL;
import java.util.List;

import lombok.SneakyThrows;
import lombok.val;

import org.icgc.dcc.etl.db.importer.tcga.model.TCGAArchiveListEntry;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;

public class TCGAArchiveListReader {

  private static final URL ARCHIVE_LISTING =
      getUrl("http://tcga-data.nci.nih.gov/datareports/resources/latestarchive");

  public static Iterable<TCGAArchiveListEntry> readEntries() {
    val entries = ImmutableList.<TCGAArchiveListEntry> builder();

    val lines = readLines();
    for (val line : lines) {
      val fields = parseFields(line);
      val archiveName = fields.get(0);
      val dateAdded = fields.get(1);
      val archiveUrl = fields.get(2);

      entries.add(new TCGAArchiveListEntry(archiveName, dateAdded, archiveUrl));
    }

    return entries.build();
  }

  @SneakyThrows
  private static Iterable<String> readLines() {
    val headerLineCount = 1;
    return skip(Resources.readLines(ARCHIVE_LISTING, UTF_8), headerLineCount);
  }

  private static List<String> parseFields(String line) {
    return TAB.splitToList(line);
  }

}