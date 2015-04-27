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
package org.icgc.dcc.etl.db.importer.cghub;

import static org.icgc.dcc.etl.db.importer.cghub.model.CGHubProjects.getProjects;

import java.io.File;
import java.io.InputStream;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.cghub.core.CGHubProcessor;
import org.icgc.dcc.etl.db.importer.cghub.reader.CGHubReader;
import org.icgc.dcc.etl.db.importer.cghub.reader.CGHubReader.DocumentCallback;
import org.icgc.dcc.etl.db.importer.cghub.util.CGHubCache;
import org.icgc.dcc.etl.db.importer.cghub.writer.CGHubWriter;

import com.google.common.base.Stopwatch;
import com.mongodb.BasicDBObject;

@Slf4j
public class CGHubImporter {

  /**
   * The HTTP service used to interact with CGHub.
   */
  @NonNull
  private final CGHubProcessor processor;

  /**
   * The local path to the cache of CGHub XML responses.
   */
  @NonNull
  private final CGHubCache cache;
  @NonNull
  private final CGHubReader reader;
  @NonNull
  private final CGHubWriter writer;

  public CGHubImporter(String mongoUri, File cacheDir) {
    this.processor = new CGHubProcessor();
    this.cache = new CGHubCache(cacheDir);
    this.reader = new CGHubReader();
    this.writer = new CGHubWriter(mongoUri);
  }

  public void execute() {
    log.info("Starting import...");
    val watch = Stopwatch.createStarted();

    log.info("Dropping previous collection, if any...");
    writer.drop();

    try {
      for (val project : getProjects()) {
        log.info("Importing project '{}'...", project);
        importProject(project);
        log.info("Finished importing project '{}'.", project);
      }
    } finally {
      val count = writer.getCount();
      log.info("Finished importing {} records in {}.", count, watch.stop());
    }
  }

  private void importProject(String project) {
    val inputStream = read(project);

    reader.parse(inputStream, new DocumentCallback() {

      @Override
      public void handle(BasicDBObject document) {
        // Persist
        writer.insert(document);
      }

    });
  }

  @SneakyThrows
  private InputStream read(String projectId) {
    if (!cache.isCached(projectId)) {
      val inputStream = processor.getProject(projectId);
      cache.cacheFile(inputStream, projectId);
    }

    return cache.readCache(projectId);
  }

}
