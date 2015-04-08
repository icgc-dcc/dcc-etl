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
package org.icgc.dcc.etl.db.importer.diagram;

import static com.google.common.base.Stopwatch.createStarted;

import java.io.IOException;
import java.net.UnknownHostException;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.diagram.model.DiagramModel;
import org.icgc.dcc.etl.db.importer.diagram.reader.DiagramReader;

import com.mongodb.MongoClientURI;

/**
 * 
 */
@Slf4j
@RequiredArgsConstructor
public class DiagramImporter {

  @NonNull
  private final MongoClientURI mongoUri;

  @SneakyThrows
  public void execute() {
    val watch = createStarted();

    log.info("Reading diagram model...");
    val model = readDiagramModel();

    log.info("Writing diagram model to {}...", mongoUri);
    writeDiagramModel(model);

    log.info("Finished importing diagrams in {}", watch);
  }

  private DiagramModel readDiagramModel() throws Exception {
    return new DiagramReader().read();
  }

  private void writeDiagramModel(DiagramModel model) throws UnknownHostException, IOException {
    // @Cleanup
    // write stuff
  }

}
