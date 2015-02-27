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
package org.icgc.dcc.etl.db.importer.go.util;

import static com.google.common.base.Objects.firstNonNull;
import static lombok.AccessLevel.PRIVATE;
import static org.obolibrary.oboformat.parser.OBOFormatConstants.OboFormatTag.TAG_ALT_ID;
import static org.obolibrary.oboformat.parser.OBOFormatConstants.OboFormatTag.TAG_DEF;
import static org.obolibrary.oboformat.parser.OBOFormatConstants.OboFormatTag.TAG_INTERSECTION_OF;
import static org.obolibrary.oboformat.parser.OBOFormatConstants.OboFormatTag.TAG_IS_A;
import static org.obolibrary.oboformat.parser.OBOFormatConstants.OboFormatTag.TAG_IS_OBSELETE;
import static org.obolibrary.oboformat.parser.OBOFormatConstants.OboFormatTag.TAG_NAME;
import static org.obolibrary.oboformat.parser.OBOFormatConstants.OboFormatTag.TAG_NAMESPACE;
import static org.obolibrary.oboformat.parser.OBOFormatConstants.OboFormatTag.TAG_RELATIONSHIP;
import static org.obolibrary.oboformat.parser.OBOFormatConstants.OboFormatTag.TAG_SYNONYM;
import static org.obolibrary.oboformat.parser.OBOFormatConstants.OboFormatTag.TAG_UNION_OF;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.etl.db.importer.go.model.GoTerm;
import org.obolibrary.oboformat.model.Frame;
import org.obolibrary.oboformat.parser.OBOFormatConstants.OboFormatTag;

import com.google.common.collect.ImmutableList;

@NoArgsConstructor(access = PRIVATE)
public final class GoTermConverter {

  /**
   * Constants.
   */
  private static final String GO_TERM_PREFIX = "GO:";
  private static final String PART_OF_VALUE = "part_of";

  public static GoTerm convertTermFrame(@NonNull Frame termFrame) {
    return GoTerm.builder()
        .id(termFrame.getId())
        .name(termFrame.getTagValue(TAG_NAME, String.class))
        .namespace(termFrame.getTagValue(TAG_NAMESPACE, String.class))
        .altIds(termFrame.getTagValues(TAG_ALT_ID, String.class))
        .def(termFrame.getTagValue(TAG_DEF, String.class))
        .synonym(termFrame.getTagValues(TAG_SYNONYM, String.class))
        .isA(termFrame.getTagValues(TAG_IS_A, String.class))
        .intersectionOf(convertTermReference(termFrame, TAG_INTERSECTION_OF))
        .unionOf(convertTermReference(termFrame, TAG_UNION_OF))
        .relationship(convertTermReference(termFrame, TAG_RELATIONSHIP))
        .obsolete(firstNonNull(termFrame.getTagValue(TAG_IS_OBSELETE, Boolean.class), false))
        .build();
  }

  private static Iterable<String> convertTermReference(Frame termFrame, OboFormatTag tag) {
    val goIds = ImmutableList.<String> builder();
    for (val clause : termFrame.getClauses(tag)) {
      // TODO: Verify
      val values = clause.getValues();
      val partOf = values.contains(PART_OF_VALUE);
      if (!partOf) {
        continue;
      }

      for (val obj : values) {
        val value = obj.toString();
        if (isGoId(value)) {
          goIds.add(value);
        }
      }
    }

    return goIds.build();
  }

  private static boolean isGoId(String value) {
    return value.startsWith(GO_TERM_PREFIX);
  }
}
