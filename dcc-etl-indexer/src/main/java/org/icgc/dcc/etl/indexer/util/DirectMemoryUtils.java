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
package org.icgc.dcc.etl.indexer.util;

import static lombok.AccessLevel.PRIVATE;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;

import lombok.NoArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.Lists;

@Slf4j
@NoArgsConstructor(access = PRIVATE)
public final class DirectMemoryUtils {

  /**
   * Constants.
   */
  private static final String MAX_DIRECT_MEMORY_PARAM = "-XX:MaxDirectMemorySize=";
  private static final long DEFAULT_SIZE = getDefaultDirectMemorySize();

  public static long getDirectMemorySize() {
    val runtime = ManagementFactory.getRuntimeMXBean();
    val arguments = Lists.reverse(runtime.getInputArguments());

    long multiplier = 1; // for the byte case.
    for (val value : arguments) {
      if (value.contains(MAX_DIRECT_MEMORY_PARAM)) {
        String memSize = value.toLowerCase().replace(MAX_DIRECT_MEMORY_PARAM.toLowerCase(), "").trim();

        if (memSize.contains("k")) {
          multiplier = 1024;
        }
        else if (memSize.contains("m")) {
          multiplier = 1048576;
        }
        else if (memSize.contains("g")) {
          multiplier = 1073741824;
        }
        memSize = memSize.replaceAll("[^\\d]", "");
        long retValue = Long.parseLong(memSize);

        return retValue * multiplier;
      }
    }
    return DEFAULT_SIZE;
  }

  private static long getDefaultDirectMemorySize() {
    try {
      Class<?> VM = Class.forName("sun.misc.VM");
      Method maxDirectMemory = VM.getDeclaredMethod("maxDirectMemory", (Class<?>) null);
      Object result = maxDirectMemory.invoke(null, (Object[]) null);
      if (result != null && result instanceof Long) {
        return (Long) result;
      }
    } catch (Exception e) {
      log.info("Unable to get maxDirectMemory from VM: {}: {}", e.getClass().getSimpleName(), e.getMessage());
    }

    // Default according to VM.maxDirectMemory()
    return Runtime.getRuntime().maxMemory();
  }

}
