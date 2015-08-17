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
package org.icgc.dcc.etl.repo.aws;

import static org.icgc.dcc.etl.repo.model.RepositorySource.AWS;

import java.util.Set;

import org.icgc.dcc.etl.repo.aws.reader.AWSS3TransferJobReader;
import org.icgc.dcc.etl.repo.aws.writer.AWSS3ObjectIdWriter;
import org.icgc.dcc.etl.repo.core.AbstractRepositorySourceFileImporter;
import org.icgc.dcc.etl.repo.core.RepositoryFileContext;

import com.google.common.collect.ImmutableSet;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AWSImporter extends AbstractRepositorySourceFileImporter {

  public AWSImporter(@NonNull RepositoryFileContext context) {
    super(AWS, context);
  }

  @Override
  public void execute() {
    log.info("Reading object ids...");
    val objectIds = readObjectIds();
    log.info("Read {} object ids", objectIds.size());

    log.info("Writing object ids...");
    writeObjectIds(objectIds);
  }

  private Set<String> readObjectIds() {
    val reader = new AWSS3TransferJobReader();

    val objectIds = ImmutableSet.<String> builder();
    for (val job : reader.read()) {
      for (val file : job.withArray("files")) {
        val objectId = file.get("object_id").textValue();

        objectIds.add(objectId);
      }
    }

    return objectIds.build();
  }

  @SneakyThrows
  private void writeObjectIds(Set<String> objectIds) {
    @Cleanup
    val writer = new AWSS3ObjectIdWriter(context.getMongoUri());
    writer.write(objectIds);
  }

}
