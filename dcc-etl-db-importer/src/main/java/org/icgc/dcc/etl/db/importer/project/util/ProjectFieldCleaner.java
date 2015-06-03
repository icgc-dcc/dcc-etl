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
package org.icgc.dcc.etl.db.importer.project.util;

import static lombok.AccessLevel.PRIVATE;
import lombok.NoArgsConstructor;
import lombok.val;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

/**
 * A temporary class that provides utility methods to purify incoming strings. Removes HTML ampersands trailing
 * whitespaces etc.
 */
@NoArgsConstructor(access = PRIVATE)
public final class ProjectFieldCleaner {

  /**
   * Constants
   */
  private static final String HTML_AMPPERSAND = "&amp;";
  private static final String AMPERSAND = "&";
  private static final String SPACE = " ";

  private static final Splitter SPLITTER = Splitter.on(SPACE);
  private static final Joiner JOINER = Joiner.on(SPACE);

  public static String cleanField(String fieldValue) {
    if (fieldValue == null) return null;

    val tokens = SPLITTER.omitEmptyStrings()
        .trimResults()
        .splitToList(fieldValue.replace(HTML_AMPPERSAND, AMPERSAND));

    return cleanSpelling(JOINER.join(tokens));
  }

  private static String cleanSpelling(String fieldValue) {
    String result = fieldValue.replace("Non Hodgkin", "Non-Hodgkin");
    result = result.replace("And", "and");
    result = result.replaceFirst("Cancer$", "cancer");

    return result;
  }

}
