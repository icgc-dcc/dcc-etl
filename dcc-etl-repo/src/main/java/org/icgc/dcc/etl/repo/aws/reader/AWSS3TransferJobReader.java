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

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.InvalidRemoteException;
import org.eclipse.jgit.api.errors.TransportException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AWSS3TransferJobReader {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String GIT_REPO_URL = "https://github.com/ICGC-TCGA-PanCancer/s3-transfer-operations.git";

  @SneakyThrows
  public List<ObjectNode> read() {
    val repoDir = Files.createTempDir();
    clone(repoDir);
    val completedDir = getCompletedDir(repoDir);

    val jobFiles = getJobFiles(completedDir);
    log.info("Reading {} completed jobs from {}...", jobFiles.size(), completedDir);

    return readFiles(jobFiles);
  }

  private List<ObjectNode> readFiles(List<File> files) throws IOException, JsonProcessingException {
    val jobs = ImmutableList.<ObjectNode> builder();

    for (val file : files) {
      log.info("Reading {}...", file);
      val job = readFile(file);

      jobs.add(job);
    }

    return jobs.build();
  }

  private File getCompletedDir(File repoDir) {
    // val completedDir = new File(new File(repoDir, "s3-transfer-jobs"), "completed-jobs");
    return new File(new File(repoDir, "pre-production"), "completed-jobs");
  }

  private List<File> getJobFiles(File completedDir) {
    return ImmutableList.copyOf(completedDir.listFiles(fileName -> fileName.getName().endsWith(".json")));
  }

  private ObjectNode readFile(File jsonFile) throws IOException, JsonProcessingException {
    return (ObjectNode) MAPPER.readTree(jsonFile);
  }

  private void clone(File directory) throws GitAPIException, InvalidRemoteException, TransportException {
    log.info("Cloning {} to {}...", GIT_REPO_URL, directory);
    Git
        .cloneRepository()
        .setURI(GIT_REPO_URL)
        .setDirectory(directory)
        .call();
  }

}
