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
package org.icgc.dcc.etl.summarizer.core;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Stopwatch.createStarted;
import static java.util.stream.Collectors.toMap;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SETS_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SET_ID;
import static org.icgc.dcc.common.core.util.Formats.formatCount;
import static org.icgc.dcc.common.core.util.Formats.formatRate;

import java.util.List;
import java.util.Map;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.util.Joiners;
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
    log.info("Resolving Id-Type counts...");
    val idTypeCount = map(repository.getGeneSetIdTypeCounts());
    for (val geneSet : geneSets) {
      val id = geneSet.get(GENE_SET_ID).textValue();
      val type = geneSet.get(GENE_SETS_TYPE).textValue();

      // Count
      val geneCount = getGeneSetGeneCount(idTypeCount, id, type);

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

  private static long getGeneSetGeneCount(Map<String, Long> idTypeCount, String id, String type) {
    val key = Joiners.DASH.join(id, type);

    return firstNonNull(idTypeCount.get(key), 0L);
  }

  private static Map<String, Long> map(List<ObjectNode> idTypeCounts) {
    return idTypeCounts.stream()
        .collect(toMap(on -> on.get("idType").textValue(), on -> on.get("count").asLong()));
  }

  private static void addSummary(ObjectNode geneSet, long geneCount) {
    val summary = createSummary();
    summary.put(GENE_SET_SUMMARY_GENE_COUNT, geneCount);

    geneSet.put(GENE_SET_SUMMARY, summary);
  }

}
