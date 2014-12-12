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
package org.icgc.dcc.etl.importer.pathway.writer;

import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.etl.importer.geneset.model.GeneSetType.PATHWAY;
import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.importer.geneset.writer.AbstractGeneSetWriter;
import org.icgc.dcc.etl.importer.pathway.core.PathwayModel;
import org.icgc.dcc.etl.importer.pathway.util.PathwayGeneSetBuilder;
import org.jongo.MongoCollection;

@Slf4j
public class PathwayGeneSetWriter extends AbstractGeneSetWriter {

  public PathwayGeneSetWriter(@NonNull MongoCollection geneSetCollection) {
    super(geneSetCollection);
  }

  public void write(@NonNull PathwayModel model) {
    log.info("Clearing pathway gene set documents...");
    clearGeneSets(PATHWAY);

    log.info("Creating gene GO gene sets builder...");
    val geneSetBuilder = new PathwayGeneSetBuilder(model);

    log.info("Writing pathway gene set documents...");
    int updatePathwayCount = 0;
    for (val reactomeId : model.getReactomeIds()) {
      val geneSet = geneSetBuilder.build(reactomeId);

      saveGeneSet(geneSet);
      updatePathwayCount++;

      val status = updatePathwayCount % 100 == 0;
      if (status) {
        log.info("Saved {} pathway documents", formatCount(updatePathwayCount));
      }
    }
  }

}
