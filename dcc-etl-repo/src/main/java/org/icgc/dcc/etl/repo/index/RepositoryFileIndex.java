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
package org.icgc.dcc.etl.repo.index;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.io.Resources.getResource;
import static java.lang.String.format;
import static org.icgc.dcc.common.core.util.Jackson.DEFAULT;
import static org.icgc.dcc.common.core.util.VersionUtils.getScmInfo;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;

import lombok.val;

import org.joda.time.DateTime;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

/**
 * Repository file index specific conventions, metadata and utilities.
 */
public class RepositoryFileIndex {

  /**
   * Index naming.
   */
  public static final String REPO_INDEX_ALIAS = "icgc-repository";
  public static final DateTimeFormatter INDEX_NAME_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

  /**
   * Index types.
   */
  public static final String INDEX_TYPE_FILE_NAME = "file";
  public static final String INDEX_TYPE_FILE_TEXT_NAME = "file-text";
  public static final List<String> INDEX_TYPE_NAMES = ImmutableList.of(
      INDEX_TYPE_FILE_NAME,
      INDEX_TYPE_FILE_TEXT_NAME);

  /**
   * Metadata location.
   */
  private static final String ES_CONFIG_BASE_PATH = "mappings";

  public static ObjectNode getSettings() throws IOException {
    val resourceName = format("%s/index.settings.json", ES_CONFIG_BASE_PATH);
    val settingsFileUrl = getResource(resourceName);

    return (ObjectNode) DEFAULT.readTree(settingsFileUrl);
  }

  public static ObjectNode getTypeMapping(String typeName) throws JsonProcessingException, IOException {
    val resourceName = format("%s/%s.mapping.json", ES_CONFIG_BASE_PATH, typeName);
    val mappingFileUrl = getResource(resourceName);
    val typeMapping = DEFAULT.readTree(mappingFileUrl);

    // Add meta data for index-to-indexer traceability
    val _meta = (ObjectNode) typeMapping.get(typeName).with("_meta");
    _meta.put("creation_date", DateTime.now().toString());
    for (val entry : getScmInfo().entrySet()) {
      val key = entry.getKey();
      val value = nullToEmpty(entry.getValue()).replaceAll("\n", " ");
      _meta.put(key, value);
    }

    return (ObjectNode) typeMapping;
  }

  public static String getCurrentIndexName() {
    val currentDate = INDEX_NAME_DATE_FORMAT.format(LocalDate.now());
    return REPO_INDEX_ALIAS + "-" + currentDate;
  }

  public static LocalDate getIndexDate(String indexName) {
    val date = indexName.replace(REPO_INDEX_ALIAS + "-", "");
    return INDEX_NAME_DATE_FORMAT.parse(date, LocalDate::from);
  }

  public static Predicate<? super String> isRepoIndexName() {
    return indexName -> indexName.startsWith(REPO_INDEX_ALIAS);
  }

  public static Comparator<? super String> compareIndexDateDescending() {
    return (repoIndexA, repoIndexB) -> {
      return -getIndexDate(repoIndexA).compareTo(getIndexDate(repoIndexB)); // Time descending
    };
  }

}