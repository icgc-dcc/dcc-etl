/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.db.importer.diagram.reader;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import lombok.val;

import org.junit.Test;

public class DiagramHighlightReaderTest {

  DiagramHighlightReader reader = new DiagramHighlightReader();

  @Test
  public void testHighlightRead1() throws IOException {
    val ids = reader.readHighlights("1300645");
    assertThat(ids)
        .containsOnly("1297354", "1297333");
  }

  @Test
  public void testHighlightRead2() throws IOException {
    val ids = reader.readHighlights("418360");
    assertThat(ids)
        .containsOnly("425661", "434798", "426223", "139855", "434700", "418359", "139854", "139853", "418309",
            "418365");
  }

  @Test
  public void testNoHighlightsRead() throws IOException {
    val ids = reader.readHighlights("167168");
    assertThat(ids).isEmpty();
  }

}
