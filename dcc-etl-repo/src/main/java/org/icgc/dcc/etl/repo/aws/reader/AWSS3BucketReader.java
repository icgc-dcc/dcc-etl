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

import static java.lang.String.format;

import java.io.File;
import java.util.List;
import java.util.function.Consumer;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.repo.model.RepositoryFile;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableList;

@Slf4j
public class AWSS3BucketReader {

  /**
   * Constants.
   */
  private static final String DEFAULT_BUCKET_NAME = "oicr.icgc.collaboratory";

  public List<RepositoryFile> read() {
    val bucketName = DEFAULT_BUCKET_NAME;
    val prefix = "data";
    val files = ImmutableList.<RepositoryFile> builder();

    readBucket(bucketName, prefix, (summary) -> {
      String fileName = getFileName(summary);

      if (isEntityId(fileName)) {
        files.add(createFile(summary));
      }
    });

    return files.build();
  }

  private RepositoryFile createFile(S3ObjectSummary summary) {
    val id = getFileName(summary);
    log.info(
        "Bucket entry: {}",
        format("%-30s %-50s %10d %s",
            id,
            summary.getKey(),
            summary.getSize(),
            summary.getStorageClass()));

    val file = new RepositoryFile()
        .setId(id);

    file.getRepository()
        .setRepoOrg("ICGC")
        .setFileSize(summary.getSize());

    return file;
  }

  private void readBucket(String bucketName, String prefix, Consumer<S3ObjectSummary> callback) {
    val s3 = createS3Client();

    val request = new ListObjectsRequest().withBucketName(bucketName).withPrefix(prefix);
    ObjectListing listing;
    do {
      listing = s3.listObjects(request);
      for (val summary : listing.getObjectSummaries()) {
        callback.accept(summary);
      }

      request.setMarker(listing.getNextMarker());
    } while (listing.isTruncated());
  }

  private boolean isEntityId(String fileName) {
    return fileName.matches("[0-9a-fA-F]{24}");
  }

  private String getFileName(S3ObjectSummary summary) {
    return new File(summary.getKey()).getName();
  }

  private AmazonS3 createS3Client() {
    return new AmazonS3Client();
  }

}
