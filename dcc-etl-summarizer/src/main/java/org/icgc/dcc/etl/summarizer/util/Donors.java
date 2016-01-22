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
package org.icgc.dcc.etl.summarizer.util;

import static com.google.common.primitives.Ints.tryParse;
import static java.lang.String.format;
import static lombok.AccessLevel.PRIVATE;
import lombok.NoArgsConstructor;
import lombok.val;

/**
 * Common utilities for working with donors.
 */
@NoArgsConstructor(access = PRIVATE)
public final class Donors {

  private static final int MIN_DONOR_AGE = 0;

  public static String getAgeGroup(String age) {
    Integer value = age == null ? null : tryParse(age);

    String ageGroup = null;
    if (value != null) {
      val interval = 10;

      // Produce values of the form: "1 - 9", "10 - 19", ...
      int groupStart = (value / interval) * interval;
      int groupEnd = groupStart + interval - 1;
      ageGroup = formatAgeGroup(groupStart == MIN_DONOR_AGE ? 1 : groupStart, groupEnd);
    }

    return ageGroup;
  }

  private static String formatAgeGroup(int groupStart, int groupEnd) {
    return format("%s - %s", groupStart, groupEnd);
  }

}
