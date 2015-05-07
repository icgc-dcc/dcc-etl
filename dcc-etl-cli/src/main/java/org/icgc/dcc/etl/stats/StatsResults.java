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
package org.icgc.dcc.etl.stats;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.padEnd;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static java.lang.String.format;
import static java.util.Arrays.asList;

import java.util.Map;
import java.util.Map.Entry;

import lombok.Data;

/**
 * Contains the stats gathered by querying the various entities of the system and reports them.
 * <p>
 * TODO: split this for each type: hdfs/mongo/es
 */
@Data
public class StatsResults {

  // TODO: CREATE/use enums?
  private static final String HDFS = "HDFS";

  private static final String MONGO = "Mongo";

  private static final String ES = "ES";

  private static final String DONOR = "donor";

  private static final String OBSERVATION_DONOR = "observationDonor";

  private static final String OBSERVATION = "observation";

  private static final String MUTATION = "mutation";

  Map<String, Long> hdfsDonorCounts = newLinkedHashMap();

  long mongoDonorCount;

  long mongoObservationDonorCount;

  long mongoObservationCount;

  long mongoMutationCount;

  long esDonorCount;

  long esObservationDonorCount;

  long esObservationCount;

  long esMutationCount;

  public void validateResults() {
    long hdfsDonorCount = hdfsDonorCount();

    checkCounts(hdfsDonorCount, mongoDonorCount, HDFS, DONOR, MONGO, DONOR);
    checkCounts(hdfsDonorCount, esDonorCount, HDFS, DONOR, ES, DONOR);

    checkState(mongoMutationCount <= mongoObservationCount,
        "Invalid mutation/observation counts", this);

    checkCounts(mongoObservationDonorCount, esObservationDonorCount, MONGO, OBSERVATION_DONOR, ES, OBSERVATION_DONOR);
    checkCounts(mongoObservationCount, esObservationCount, MONGO, OBSERVATION, ES, OBSERVATION);
    checkCounts(mongoMutationCount, esMutationCount, MONGO, MUTATION, ES, MUTATION);
  }

  private void checkCounts(long count1, long count2, String... desc) {
    checkState(count1 != 0 && count1 == count2,
        "Invalid counts: zero counts or '%s' != '%s' for '%s'",
        count1, count2, asList(desc));
  }

  public String displayableResults() {
    StringBuilder sb = new StringBuilder();
    sb.append(displayableHhdfsResults());
    sb.append(displayableMongoResults());
    sb.append(displayableEsResults());
    return sb.toString();
  }

  private String displayableHhdfsResults() {
    StringBuilder sb = new StringBuilder();
    sb.append(format("\t%s:%n", HDFS));
    sb.append(format("\t\t%s%s%n", pad(HDFS, DONOR), hdfsDonorCount()));
    for (Entry<String, Long> entry : hdfsDonorCounts.entrySet()) {
      sb.append(format("\t\t%s%s%n", pad("  " + HDFS, entry.getKey()), entry.getValue()));
    }
    sb.append("\n");
    return sb.toString();
  }

  private String displayableMongoResults() {
    StringBuilder sb = new StringBuilder();
    sb.append(format("\t%s:%n", MONGO));
    sb.append(format("\t\t%s%s%n", pad(MONGO, DONOR), mongoDonorCount));
    sb.append(format("\t\t%s%s%n", pad(MONGO, OBSERVATION_DONOR), mongoObservationDonorCount));
    sb.append(format("\t\t%s%s%n", pad(MONGO, OBSERVATION), mongoObservationCount));
    sb.append(format("\t\t%s%s%n", pad(MONGO, MUTATION), mongoMutationCount));
    sb.append("\n");
    return sb.toString();
  }

  private String displayableEsResults() {
    StringBuilder sb = new StringBuilder();
    sb.append(format("\t%s:%n", ES));
    sb.append(format("\t\t%s%s%n", pad(ES, DONOR), esDonorCount));
    sb.append(format("\t\t%s%s%n", pad(ES, OBSERVATION_DONOR), esObservationDonorCount));
    sb.append(format("\t\t%s%s%n", pad(ES, OBSERVATION), esObservationCount));
    sb.append(format("\t\t%s%s%n", pad(ES, MUTATION), esMutationCount));
    sb.append("\n");
    return sb.toString();
  }

  private long hdfsDonorCount() {
    long sum = 0;
    for (Long l : hdfsDonorCounts.values()) {
      sum += l;
    }
    return sum;
  }

  private String pad(String store, String entity) {
    int minLength = 32;
    return padEnd(
        format("%s/%s count:",
            store,
            entity),
        minLength,
        ' ');
  }

}
