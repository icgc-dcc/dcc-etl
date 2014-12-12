/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.annotator.model;

import static org.icgc.dcc.etl.core.model.Component.ANNOTATOR;

import java.util.Collection;

import lombok.NonNull;
import lombok.Value;
import lombok.val;
import lombok.experimental.Builder;

import org.apache.hadoop.fs.Path;

import com.google.common.collect.ImmutableList;

@Value
@Builder
public class AnnotatorJob {

  String workingDir;
  Iterable<String> projectNames;
  Iterable<AnnotatedFileType> fileTypes;
  Collection<Path> files;

  public Collection<Path> getFilesByProject(String projectName) {
    val result = new ImmutableList.Builder<Path>();
    for (val file : files) {
      if (file.getParent().getName().equals(projectName)) {
        result.add(file);
      }
    }

    return result.build();
  }

  public Path getOutputPath(@NonNull String projectName, @NonNull AnnotatedFileType fileType) {
    val annotatorDir = new Path(workingDir, ANNOTATOR.getDirName());
    val projectDir = new Path(annotatorDir, projectName);

    return new Path(projectDir, fileType.getOutputFileName());
  }

  public boolean isEmpty() {
    return files.isEmpty();
  }

}
