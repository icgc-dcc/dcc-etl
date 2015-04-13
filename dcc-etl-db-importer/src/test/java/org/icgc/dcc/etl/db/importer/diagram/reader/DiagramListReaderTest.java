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

import javax.xml.transform.TransformerException;

import lombok.val;

import org.junit.Test;

public class DiagramListReaderTest {

  @Test
  public void testReadList() throws IOException, TransformerException {
    val reader = new DiagramListReader();
    val pathways = reader.readPathwayList();

    assertThat(pathways.getDiagrammed()).isNotEmpty();

    // This might be too aggressive of a test - but at least it fails when something changes?
    assertThat(pathways.getDiagrammed().size()).isEqualTo(471);
    assertThat(pathways.getNotDiagrammed().size()).isEqualTo(1182);
  }

  @Test
  public void testIdConvert() throws IOException {
    val reader = new DiagramListReader();
    val result = reader.getReactId("1300645");
    assertThat(result).isEqualTo("REACT_163938");
  }

}
