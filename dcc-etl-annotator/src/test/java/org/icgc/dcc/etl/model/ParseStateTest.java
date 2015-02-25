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
package org.icgc.dcc.etl.model;

import static org.assertj.core.api.Assertions.assertThat;

import lombok.val;

import org.icgc.dcc.etl.annotator.model.ParseNotification;
import org.icgc.dcc.etl.annotator.model.ParseState;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ParseStateTest {

  private static final ParseNotification ERROR_CODE = ParseNotification.ERROR_CHROMOSOME_NOT_FOUND;
  private static final ParseNotification ERROR_CODE_2 = ParseNotification.ERROR_MISSING_CDS_SEQUENCE;
  private static final String ERROR_MESSAGE = "error message";
  private static final String ERROR_MESSAGE_2 = "error message 2";

  @Test
  public void emptyConstructorTest() {
    val object = new ParseState();
    assertThat(object.getErrorCodes()).isEmpty();
    assertThat(object.getErrorMessages()).isEmpty();
  }

  @Test
  public void nonEmptyConstructorTest() {
    val object = new ParseState(ERROR_CODE, ERROR_MESSAGE);
    assertThat(object.getErrorCodes()).isNotEmpty();
    assertThat(object.getErrorMessages()).isNotEmpty();
    assertThat(object.hasError()).isTrue();
    assertThat(object.getErrorCodes().size()).isEqualTo(1);
    assertThat(object.getErrorMessages().size()).isEqualTo(1);
    assertThat(object.getErrorCodes().get(0)).isEqualTo(ERROR_CODE);
    assertThat(object.getErrorMessages().get(0)).isEqualTo(ERROR_MESSAGE);
  }

  @Test
  public void templateConstuctorTest() {
    val object = new ParseState(ERROR_CODE, "This is %s", ERROR_MESSAGE);
    assertThat(object.getErrorCodes().get(0)).isEqualTo(ERROR_CODE);
    assertThat(object.getErrorMessages().get(0)).isEqualTo(String.format("This is %s", ERROR_MESSAGE));
  }

  @Test
  public void listConstructorTest() {
    val codesList = Lists.newArrayList(ERROR_CODE, ERROR_CODE_2);
    val messagesList = Lists.newArrayList(ERROR_MESSAGE, ERROR_MESSAGE_2);

    val object = new ParseState(codesList, messagesList);
    assertThat(object.getErrorCodes().size()).isEqualTo(2);
    assertThat(object.getErrorMessages().size()).isEqualTo(2);
    assertThat(object.getErrorCodes().get(0)).isEqualTo(ERROR_CODE);
    assertThat(object.getErrorCodes().get(1)).isEqualTo(ERROR_CODE_2);
    assertThat(object.getErrorMessages().get(0)).isEqualTo(ERROR_MESSAGE);
    assertThat(object.getErrorMessages().get(1)).isEqualTo(ERROR_MESSAGE_2);
  }

  @Test
  public void addErrorCodeTest() {
    val object = new ParseState();
    object.addErrorCode(ERROR_CODE);
    assertThat(object.getErrorCodes()).containsOnly(ERROR_CODE);
  }

  @Test
  public void addErrorMessageTest() {
    val object = new ParseState();
    object.addErrorMessage(ERROR_MESSAGE);
    assertThat(object.getErrorMessages()).containsOnly(ERROR_MESSAGE);
  }

  @Test
  public void addAllMessagesTest() {
    val object = new ParseState();
    object.addAllMessages(Lists.newArrayList(ERROR_MESSAGE, ERROR_MESSAGE_2));
    assertThat(object.getErrorMessages()).containsOnly(ERROR_MESSAGE, ERROR_MESSAGE_2);
  }

  @Test
  public void addErrorAndMessageTest() {
    val object = new ParseState();
    object.addErrorAndMessage(ERROR_CODE, ERROR_MESSAGE);
    assertThat(object.getErrorCodes()).containsOnly(ERROR_CODE);
    assertThat(object.getErrorMessages()).containsOnly(ERROR_MESSAGE);
  }

  @Test
  public void addErrorAndMessageTemplateTest() {
    val object = new ParseState();
    object.addErrorAndMessage(ERROR_CODE, "This is %s", ERROR_MESSAGE);
    assertThat(object.getErrorCodes()).containsOnly(ERROR_CODE);
    assertThat(object.getErrorMessages()).containsOnly(String.format("This is %s", ERROR_MESSAGE));
  }

  @Test
  public void hasErrorTest() {
    assertThat(new ParseState().hasError()).isFalse();
    val object = new ParseState(ERROR_CODE, ERROR_MESSAGE);
    assertThat(object.hasError()).isTrue();
    object.addErrorCode(ERROR_CODE_2);
    assertThat(object.hasError()).isTrue();
  }

  @Test
  public void containsAnyErrorTest() {
    val object = new ParseState(ERROR_CODE, ERROR_MESSAGE);
    assertThat(object.containsAnyError(ERROR_CODE)).isTrue();
    assertThat(object.containsAnyError(ERROR_CODE_2)).isFalse();

    object.addErrorCode(ERROR_CODE_2);
    assertThat(object.containsAnyError(ERROR_CODE)).isTrue();
    assertThat(object.containsAnyError(ERROR_CODE_2)).isTrue();
    assertThat(object.containsAnyError(ParseNotification.CDS_MUTATION_FAILURE)).isFalse();
    assertThat(object.containsAnyError(ParseNotification.ERROR_OUT_OF_CHROMOSOME_RANGE)).isFalse();
  }

}
