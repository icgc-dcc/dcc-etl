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
package org.icgc.dcc.etl.loader.config;

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigMergeable;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigOrigin;
import com.typesafe.config.ConfigResolveOptions;
import com.typesafe.config.ConfigValue;

/**
 * This is temporary until we remove typesafe config.
 */
public abstract class BaseConfig implements Config {

  @Override
  public ConfigObject root() {
    return null;
  }

  @Override
  public ConfigOrigin origin() {
    return null;
  }

  @Override
  public Config withFallback(ConfigMergeable other) {
    return null;
  }

  @Override
  public Config resolve() {
    return null;
  }

  @Override
  public Config resolve(ConfigResolveOptions options) {
    return null;
  }

  @Override
  public boolean isResolved() {
    return false;
  }

  @Override
  public Config resolveWith(Config source) {
    return null;
  }

  @Override
  public Config resolveWith(Config source, ConfigResolveOptions options) {
    return null;
  }

  @Override
  public void checkValid(Config reference, String... restrictToPaths) {
  }

  @Override
  public boolean hasPath(String path) {
    return false;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public Set<Entry<String, ConfigValue>> entrySet() {
    return null;
  }

  @Override
  public boolean getBoolean(String path) {
    return false;
  }

  @Override
  public Number getNumber(String path) {
    return null;
  }

  @Override
  public int getInt(String path) {
    return 0;
  }

  @Override
  public long getLong(String path) {
    return 0;
  }

  @Override
  public double getDouble(String path) {
    return 0;
  }

  @Override
  public String getString(String path) {
    return null;
  }

  @Override
  public ConfigObject getObject(String path) {
    return null;
  }

  @Override
  public Config getConfig(String path) {
    return null;
  }

  @Override
  public Object getAnyRef(String path) {
    return null;
  }

  @Override
  public ConfigValue getValue(String path) {
    return null;
  }

  @Override
  public Long getBytes(String path) {
    return null;
  }

  @Override
  public Long getMilliseconds(String path) {
    return null;
  }

  @Override
  public Long getNanoseconds(String path) {
    return null;
  }

  @Override
  public long getDuration(String path, TimeUnit unit) {
    return 0;
  }

  @Override
  public ConfigList getList(String path) {
    return null;
  }

  @Override
  public List<Boolean> getBooleanList(String path) {
    return null;
  }

  @Override
  public List<Number> getNumberList(String path) {
    return null;
  }

  @Override
  public List<Integer> getIntList(String path) {
    return null;
  }

  @Override
  public List<Long> getLongList(String path) {
    return null;
  }

  @Override
  public List<Double> getDoubleList(String path) {
    return null;
  }

  @Override
  public List<String> getStringList(String path) {
    return null;
  }

  @Override
  public List<? extends ConfigObject> getObjectList(String path) {
    return null;
  }

  @Override
  public List<? extends Config> getConfigList(String path) {
    return null;
  }

  @Override
  public List<? extends Object> getAnyRefList(String path) {
    return null;
  }

  @Override
  public List<Long> getBytesList(String path) {
    return null;
  }

  @Override
  public List<Long> getMillisecondsList(String path) {
    return null;
  }

  @Override
  public List<Long> getNanosecondsList(String path) {
    return null;
  }

  @Override
  public List<Long> getDurationList(String path, TimeUnit unit) {
    return null;
  }

  @Override
  public Config withOnlyPath(String path) {
    return null;
  }

  @Override
  public Config withoutPath(String path) {
    return null;
  }

  @Override
  public Config atPath(String path) {
    return null;
  }

  @Override
  public Config atKey(String key) {
    return null;
  }

  @Override
  public Config withValue(String path, ConfigValue value) {
    return null;
  }

}
