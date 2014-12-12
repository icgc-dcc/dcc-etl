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
package org.icgc.dcc.etl.loader.core;

import static cascading.cascade.CascadeDef.cascadeDef;
import static cascading.cascade.CascadeProps.cascadeProps;
import static com.google.common.base.Joiner.on;
import static lombok.AccessLevel.PRIVATE;

import java.util.Properties;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.cascade.CascadeDef;

import com.google.common.base.Joiner;

/**
 * Helper class for cascading in the context of the loader.
 */
@Slf4j
@NoArgsConstructor(access = PRIVATE)
final class LoaderCascadings {

  private static final Joiner JOINER = on("_");

  private static final String CASCADE_BASE_NAME = "loader-cascade";

  private static final String PIPE_BASE_NAME = "loader-pipe";

  static String pipeName(String... qualifiers) {
    return JOINER.join(PIPE_BASE_NAME, qualifiers);
  }

  static CascadeDef createCascadeDef(String... cascadeQualifiers) {
    String cascadeName = cascadeName(cascadeQualifiers);
    log.info("Creating cascade '{}'", cascadeName);
    return cascadeDef().setName(cascadeName);
  }

  static Cascade connectCascade(CascadeDef cascadeDef) {
    return new CascadeConnector(cascadeProperties()).connect(cascadeDef);
  }

  private static String cascadeName(String... qualifiers) {
    return JOINER.join(CASCADE_BASE_NAME, JOINER.join(qualifiers));
  }

  private static Properties cascadeProperties() {
    return cascadeProps().buildProperties();
  }

}
