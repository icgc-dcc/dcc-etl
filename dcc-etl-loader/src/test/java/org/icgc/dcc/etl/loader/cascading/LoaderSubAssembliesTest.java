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
package org.icgc.dcc.etl.loader.cascading;

import static org.icgc.dcc.common.hadoop.cascading.Cascades.cascadingSerialize;

import java.util.LinkedHashMap;
import java.util.Map;

import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.junit.Test;

import cascading.pipe.Pipe;
import cascading.tuple.Fields;

import com.google.common.collect.Sets;

public class LoaderSubAssembliesTest {

  private static final String PROJECT_KEY = "dummyproject";
  private static final String PIPE_NAME = "dummypipe";
  private static final Fields DUMMY_FIELD = new Fields("dummyfield");

  @Test
  public void test_serialization() {
    cascadingSerialize(new RawSequenceDataInfo(
        new Pipe(PIPE_NAME),
        PROJECT_KEY,
        Sets.<FeatureType> newLinkedHashSet(),
        new LinkedHashMap<FeatureType, Map<String, Map<String, String>>>()));
    cascadingSerialize(new RawSequenceDataInfo.Nester(DUMMY_FIELD));
    cascadingSerialize(new NullConverter(null, DUMMY_FIELD, null));
    cascadingSerialize(new CodesTranslator(null, new LinkedHashMap<String, Map<String, String>>()));
  }
}
