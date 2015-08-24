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
package org.icgc.dcc.etl.repo.aws.reader;

import static com.google.common.io.Files.createTempDir;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.InvalidRemoteException;
import org.eclipse.jgit.api.errors.TransportException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AWSS3TransferJobReader {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String GIT_REPO_URL = "https://github.com/ICGC-TCGA-PanCancer/s3-transfer-operations.git";

  @SneakyThrows
  public List<ObjectNode> read() {
    val repoDir = cloneRepo();
    val completedDir = getCompletedDir(repoDir);

    log.info("Resolving job files from completed dir '{}'...", completedDir.getCanonicalPath());
    val jobFiles = resolveJobFiles(completedDir);

    log.info("Reading {} completed jobs from {}...", formatCount(jobFiles.size()), completedDir);
    return readFiles(jobFiles);
  }

  public Set<String> readObjectIds() {
    val objectIds = ImmutableSet.<String> builder();
    for (val job : read()) {
      for (val file : job.withArray("files")) {
        val objectId = file.get("object_id").textValue();
        objectIds.add(objectId);
      }
    }

    return objectIds.build();
  }

  private List<ObjectNode> readFiles(List<File> files) throws IOException, JsonProcessingException {
    return files.stream().map(this::readFile).collect(toImmutableList());
  }

  private File getCompletedDir(File repoDir) {
    return new File(new File(repoDir, "s3-transfer-jobs"), "completed-jobs");
  }

  private List<File> resolveJobFiles(File completedDir) {
    File[] jobFiles = completedDir.listFiles(fileName -> fileName.getName().endsWith(".json"));
    if (jobFiles == null) {
      return Collections.emptyList();
    }

    return ImmutableList.copyOf(jobFiles);
  }

  @SneakyThrows
  private ObjectNode readFile(File jsonFile) {
    log.info("Reading {}...", jsonFile);
    return (ObjectNode) MAPPER.readTree(jsonFile);
  }

  private File cloneRepo() throws GitAPIException, InvalidRemoteException, TransportException {
    val repoDir = createTempDir();
    log.info("Cloning {} to {}...", GIT_REPO_URL, repoDir);
    Git
        .cloneRepository()
        .setURI(GIT_REPO_URL)
        .setDirectory(repoDir)
        .call();

    return repoDir;
  }

}
