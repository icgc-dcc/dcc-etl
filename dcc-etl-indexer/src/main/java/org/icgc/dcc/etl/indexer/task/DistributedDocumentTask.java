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
package org.icgc.dcc.etl.indexer.task;

import static org.icgc.dcc.etl.indexer.factory.JongoFactory.newJongo;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

import org.icgc.dcc.etl.indexer.core.CollectionReader;
import org.icgc.dcc.etl.indexer.core.Config;
import org.icgc.dcc.etl.indexer.io.MongoDBCollectionReader;
import org.icgc.dcc.etl.indexer.model.DocumentType;

@EqualsAndHashCode(callSuper = true)
public class DistributedDocumentTask extends AbstractDocumentTask {

  public DistributedDocumentTask(@NonNull DocumentType type, @NonNull Config config) {
    super(type, config);
  }

  @Override
  protected CollectionReader createCollectionReader() {
    // FIXME: Change this back?
    // return new DirectMemoryMongoDBCollectionReader(newJongo(config.getMongoUri()));
    return new MongoDBCollectionReader(newJongo(config.getMongoUri()));
  }

}
