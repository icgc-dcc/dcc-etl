/*
 * Copyright (c) 2013 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.summarizer.core;

import static com.google.common.base.Stopwatch.createStarted;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SETS_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SET_ID;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.FormatUtils.formatRate;
import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.summarizer.repository.ReleaseRepository;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Summarizes {@code GeneSet} collection information.
 */
@Slf4j
public class GeneSetSummarizer extends AbstractSummarizer {

  /**
   * Constants.
   */
  private static final String GENE_SET_SUMMARY = "_summary";
  private static final String GENE_SET_SUMMARY_GENE_COUNT = "_gene_count";
  private static final int COUNTER_THRESHOLD = 5000;

  public GeneSetSummarizer(@NonNull ReleaseRepository repository) {
    super(repository);
  }

  @Override
  public void summarize() {
    int count = 0;
    val watch = createStarted();

    log.info("Getting gene sets...");
    val geneSets = repository.getGeneSets();
    for (val geneSet : geneSets) {
      val id = geneSet.get(GENE_SET_ID).textValue();
      val type = geneSet.get(GENE_SETS_TYPE).textValue();

      // Count
      val geneCount = repository.getGeneSetGeneCount(id, type);

      // Update
      addSummary(geneSet, geneCount);

      // Save
      repository.saveGeneSet(geneSet);

      if (++count % COUNTER_THRESHOLD == 0) {
        log.info("Summarized {} gene set(s) ({} docs/s)",
            formatCount(count), formatRate(rate(COUNTER_THRESHOLD, watch)));
      }
    }
  }

  private static void addSummary(ObjectNode geneSet, long geneCount) {
    val summary = createSummary();
    summary.put(GENE_SET_SUMMARY_GENE_COUNT, geneCount);

    geneSet.put(GENE_SET_SUMMARY, summary);
  }

}
