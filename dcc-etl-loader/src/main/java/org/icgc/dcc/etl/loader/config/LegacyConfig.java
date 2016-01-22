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
package org.icgc.dcc.etl.loader.config;

import static org.icgc.dcc.common.core.model.Configurations.FS_ROOT_KEY;
import static org.icgc.dcc.common.core.model.Configurations.FS_URL_KEY;
import static org.icgc.dcc.common.core.model.Configurations.MONGO_URI_KEY;
import lombok.RequiredArgsConstructor;

/**
 * Extends legacy to customize {@link #getString(String)}
 */
@RequiredArgsConstructor
public class LegacyConfig extends BaseConfig {

  private final ConfigModule configModule;

  /**
   * If adding to this, make sure to also update {@link #getString(String)}.
   */
  @Override
  public boolean hasPath(String path) {
    return FS_URL_KEY.equals(path) || FS_ROOT_KEY.equals(path) || MONGO_URI_KEY.equals(path);
  }

  /**
   * If adding to this, make sure to also update {@link #hasPath(String)}.
   */
  @Override
  public String getString(String path) {
    // Support the configuration required for the dcc-submission project
    if (FS_URL_KEY.equals(path)) {
      return configModule.getFsUrl();
    }

    throw new RuntimeException("Legacy DCC path '" + path + "' not supported!");
  }

}
