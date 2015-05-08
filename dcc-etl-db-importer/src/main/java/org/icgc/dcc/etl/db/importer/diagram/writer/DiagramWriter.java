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
package org.icgc.dcc.etl.db.importer.diagram.writer;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.model.ReleaseCollection;
import org.icgc.dcc.etl.db.importer.diagram.model.DiagramModel;
import org.icgc.dcc.etl.db.importer.diagram.model.DiagramNodeConverter;
import org.icgc.dcc.etl.db.importer.util.AbstractJongoWriter;
import org.jongo.MongoCollection;

import com.mongodb.MongoClientURI;

@Slf4j
public class DiagramWriter extends AbstractJongoWriter<DiagramModel> {

  private MongoCollection diagramCollection;

  public DiagramWriter(@NonNull MongoClientURI mongoUri) {
    super(mongoUri);
  }

  @Override
  public void writeFiles(@NonNull DiagramModel value) {
    diagramCollection = getCollection(ReleaseCollection.DIAGRAM_COLLECTION);

    log.info("Droppping current diagram collection..");
    dropCollection();

    log.info("Saving new diagram collection..");
    saveCollection(value);
  }

  private void dropCollection() {
    diagramCollection.drop();
  }

  private void saveCollection(DiagramModel value) {
    val converter = new DiagramNodeConverter();

    for (val entry : value.getDiagrams().entrySet()) {
      val diagram = entry.getValue();
      val id = entry.getKey();
      diagramCollection.save(converter.convertDiagram(diagram, id));
    }
  }

}
