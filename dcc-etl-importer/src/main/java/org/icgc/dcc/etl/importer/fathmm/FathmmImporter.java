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
package org.icgc.dcc.etl.importer.fathmm;

import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.FormatUtils.formatRate;
import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.importer.fathmm.core.FathmmPredictor;
import org.icgc.dcc.etl.importer.fathmm.reader.FathmmObservationReader;
import org.icgc.dcc.etl.importer.fathmm.reader.FathmmTranscriptReader;
import org.icgc.dcc.etl.importer.fathmm.writer.FathmmObservationWriter;

import com.google.common.base.Stopwatch;
import com.mongodb.MongoClientURI;

/**
 * Import Fathmm functional impact predictions into the observations collection
 */
@Slf4j
@RequiredArgsConstructor
public class FathmmImporter {

  /**
   * Configuration.
   */
  @NonNull
  private final String fathmmPostgresqlUri;
  @NonNull
  private final MongoClientURI mongoUri;

  @SneakyThrows
  public void execute() {
    @Cleanup
    val observationReader = new FathmmObservationReader(mongoUri);
    @Cleanup
    val transcriptReader = new FathmmTranscriptReader(mongoUri);
    @Cleanup
    val observationWriter = new FathmmObservationWriter(mongoUri);
    @Cleanup
    val predictor = new FathmmPredictor(fathmmPostgresqlUri);

    val observations = observationReader.read();
    val transcripts = transcriptReader.read();

    int observationCounter = 0;
    int consequenceCounter = 0;
    val watch = Stopwatch.createStarted();
    for (val observation : observations) {
      observationCounter++;

      if (observationCounter % 10000 == 0) {
        log.info("Scanned {} observations and {} consequences. Throughput (observations) {}",
            formatCount(observationCounter),
            formatCount(consequenceCounter),
            formatRate(observationCounter, watch));
      }

      consequenceCounter += observationWriter.write(transcripts, predictor, observation);
    }

    log.info("Scanned {} observations and {} missense consequences. Throughput (observations) {} in {}",
        formatCount(observationCounter), formatCount(consequenceCounter),
        formatRate(observationCounter, watch), watch);
  }

}
