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
package org.icgc.dcc.etl.config;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.Strings2.EMPTY_STRING;
import static org.icgc.dcc.etl.config.ConfigInfoExtractor.getInfo2;

import java.io.File;

import lombok.NoArgsConstructor;
import lombok.val;

import org.icgc.dcc.etl.config.ConfigInfoExtractor.SubType;
import org.icgc.dcc.etl.config.ConfigInfoExtractor.Type;

@NoArgsConstructor(access = PRIVATE)
public class Main {

  private static final String ERROR_MESSAGE = "Expecting 3 arguments";

  /**
   * Entry point into the config parser.
   * 
   * @param args[0] Path to the config file
   * @param args[1] {@link Type} of info to be extracted from it.
   * @param args[1] {@link SubType} of info to be extracted from it.
   */
  public static void main(String... args) {
    checkArgument(checkNotNull(args, ERROR_MESSAGE).length >= 3, ERROR_MESSAGE);

    val configFilePath = checkNotNull(args[0]);
    val typeName = checkNotNull(args[1]);
    val subTypeName = checkNotNull(args[2]);

    checkState(new File(configFilePath).isFile(),
        "Expecting a valid config file path, instead got: '%s'", configFilePath);
    checkState(Type.isValid(typeName),
        "Expecting a valid type name, instead got: '%s'", typeName);
    checkState(SubType.isValid(subTypeName),
        "Expecting a valid sub type name, instead got: '%s'", subTypeName);

    val info = getInfo2(
        new File(configFilePath),
        Type.valueOf(typeName),
        SubType.valueOf(subTypeName));
    System.out.println(info.isPresent() ? info.get() : EMPTY_STRING);

  }
}