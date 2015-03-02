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
package org.icgc.dcc.etl.db.importer.go.core;

import static com.google.common.base.Stopwatch.createStarted;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.etl.db.importer.go.util.GoAssociationAggregator.aggregateAssociations;
import static org.icgc.dcc.etl.db.importer.go.util.GoAssociationFilter.filterNegativeAssociations;
import static org.icgc.dcc.etl.db.importer.go.util.GoAssociationFilter.filterPositiveAssociations;

import java.io.IOException;
import java.net.URISyntaxException;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.db.importer.go.model.GoAssociation;
import org.icgc.dcc.etl.db.importer.go.reader.GoAssociationReader;

@Slf4j
@RequiredArgsConstructor
public class GoAssociationProcessor {

  /**
   * Dependencies.
   */
  @NonNull
  private final GoAssociationReader associationReader;

  public Iterable<GoAssociation> process() throws IOException, URISyntaxException {
    val timer = createStarted();

    log.info("Reading associations...");
    val associations = readAssociations();

    log.info("Filtering positive associations...");
    val positiveAssociations = filterPositiveAssociations(associations);

    log.info("Filtering negative associations...");
    val negativeAssociations = filterNegativeAssociations(associations);

    log.info("Aggregating positive associations...");
    val aggregatedAssociations = aggregateAssociations(positiveAssociations);

    log.info("Processed {} total associations, {} positive, {} negative, {} aggregates (positive) in {}.",
        formatCount(associations),
        formatCount(positiveAssociations),
        formatCount(negativeAssociations),
        formatCount(aggregatedAssociations),
        timer);

    return aggregatedAssociations;
  }

  private Iterable<GoAssociation> readAssociations() throws IOException, URISyntaxException {
    return associationReader.read();
  }

}
