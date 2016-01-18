/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
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
package org.icgc.dcc.etl.loader.core;

import static com.google.common.collect.Lists.newArrayList;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.Optionals.ABSENT_STRING;
import static org.icgc.dcc.common.core.util.Optionals.ABSENT_STRING_LIST;
import static org.icgc.dcc.common.cascading.CascadingOptionals.ABSENT_PIPE;

import java.util.List;

import lombok.NoArgsConstructor;

import org.icgc.dcc.common.hadoop.util.HadoopCompression;

import cascading.pipe.Pipe;

import com.google.common.base.Optional;

/**
 * ICGCClientConfigs method for loader-specific {@link Optional}s (helps with verbosity).
 */
@NoArgsConstructor(access = PRIVATE)
public final class LoaderOptionals {

  public static final Optional<HadoopCompression> ABSENT_HADOOP_COMPRESSION = Optional.absent();

  public static Optional<String> optional(String string) {
    return string == null ?
        ABSENT_STRING :
        Optional.<String> of(string);
  }

  public static Optional<List<String>> optional(List<String> strings) {
    if (strings == null) {
      return ABSENT_STRING_LIST;
    } else {
      List<String> list = newArrayList(strings); // To avoid the cast
      return Optional.of(list);
    }
  }

  public static Optional<Pipe> optional(Pipe pipe) {
    return pipe == null ?
        ABSENT_PIPE :
        Optional.<Pipe> of(pipe);
  }

  public static Optional<HadoopCompression> optional(HadoopCompression hadoopCompression) {
    return hadoopCompression == null ?
        ABSENT_HADOOP_COMPRESSION :
        Optional.<HadoopCompression> of(hadoopCompression);
  }

}
