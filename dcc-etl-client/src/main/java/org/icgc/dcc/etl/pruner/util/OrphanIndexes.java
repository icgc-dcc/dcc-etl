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
package org.icgc.dcc.etl.pruner.util;

import static com.google.common.collect.Multimaps.index;
import static lombok.AccessLevel.PRIVATE;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.etl.pruner.model.Orphan;

import com.google.common.base.Function;
import com.google.common.collect.Multimap;

/**
 * Various indexing functions for dealing with a collection of orphans.
 */
@NoArgsConstructor(access = PRIVATE)
public final class OrphanIndexes {

  public static Multimap<String, Orphan> groupOrphansByProject(@NonNull Iterable<Orphan> orphans) {
    return index(orphans, new Function<Orphan, String>() {

      @Override
      public String apply(Orphan orphan) {
        return orphan.getProjectName();
      }

    });
  }

  public static Multimap<FileType, Orphan> groupOrphansByFileType(@NonNull Iterable<Orphan> orphans) {
    return index(orphans, new Function<Orphan, FileType>() {

      @Override
      public FileType apply(Orphan orphan) {
        return orphan.getFileType();
      }

    });
  }

  public static Multimap<Path, Orphan> groupOrphansByFile(@NonNull Iterable<Orphan> orphans) {
    return index(orphans, new Function<Orphan, Path>() {

      @Override
      public Path apply(Orphan orphan) {
        return orphan.getFile();
      }

    });
  }

}
