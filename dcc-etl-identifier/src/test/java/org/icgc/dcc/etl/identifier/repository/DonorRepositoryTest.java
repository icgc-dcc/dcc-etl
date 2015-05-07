/*
 * Copyright 2013(c) The Ontario Institute for Cancer Research. All rights reserved.
 * 
 * This program and the accompanying materials are made available under the terms of the GNU Public
 * License v3.0. You should have received a copy of the GNU General Public License along with this
 * program. If not, see <http://www.gnu.org/licenses/>.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.etl.identifier.repository;

import static com.google.common.collect.ImmutableMap.of;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static org.apache.commons.lang.StringUtils.removeStart;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.junit.Ignore;
import org.junit.Test;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;

import com.google.common.base.Function;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class DonorRepositoryTest extends BaseRepositoryTest<DonorRepository> {

  @Test(expected = UnableToExecuteStatementException.class)
  public void test_insertDonorId_duplicate() {
    // Setup
    String submittedDonorId = "donor1";
    String submittedProjectId = "project1";
    String creationRelease = "release1";

    // Execute
    Long id = repository.insertDonorId(submittedDonorId, submittedProjectId, creationRelease);
    assertThat(id).isEqualTo(1);

    id = repository.insertDonorId(submittedDonorId, submittedProjectId, creationRelease);
  }

  @Test(expected = UnableToExecuteStatementException.class)
  public void test_insertDonorId_duplicate_different_release() {
    // Setup
    String submittedDonorId = "donor1";
    String submittedProjectId = "project1";
    String creationRelease1 = "release1";
    String creationRelease2 = "release2";

    // Execute
    Long id = repository.insertDonorId(submittedDonorId, submittedProjectId, creationRelease1);
    assertThat(id).isEqualTo(1);

    id = repository.insertDonorId(submittedDonorId, submittedProjectId, creationRelease2);
  }

  @Test(expected = BadRequestException.class)
  public void test_findId_null_field() {
    // Setup
    String submittedDonorId = "donor1";
    String submittedProjectId = null;
    String creationRelease = "release1";

    // Execute
    String donorId = repository.findId(CREATE, submittedDonorId, submittedProjectId, creationRelease);

    // Verify
    assertThat(donorId).isEqualTo("DO1");
  }

  @Test
  public void test_findId_non_existing() {
    // Setup
    String submittedDonorId = "donor1";
    String submittedProjectId = "project1";
    String creationRelease = "release1";

    // Execute
    String donorId = repository.findId(CREATE, submittedDonorId, submittedProjectId, creationRelease);

    // Verify
    assertThat(donorId).isEqualTo("DO1");
  }

  @Test
  public void test_findId_keep_previous_release_name() {
    // Setup
    String submittedDonorId = "donor1";
    String submittedProjectId = "project1";
    String creationRelease1 = "release1";
    String creationRelease2 = "release2";

    // Execute
    String donorId = repository.findId(CREATE, submittedDonorId, submittedProjectId, creationRelease1);

    // Verify
    assertThat(donorId).isEqualTo("DO1");

    // Execute
    donorId = repository.findId(CREATE, submittedDonorId, submittedProjectId, creationRelease2);

    assertThat(donorId).isEqualTo("DO1");

    String actualRelease = getCreationRelease("donor_ids", 1);

    assertThat(actualRelease).isEqualTo(creationRelease1);
  }

  @Test
  public void test_findId_existing() {
    // Setup
    String submittedDonorId = "donor1";
    String submittedProjectId = "project1";
    String creationRelease = "release1";
    insertDonor(submittedDonorId, submittedProjectId, creationRelease);

    // Execute
    String donorId = repository.findId(CREATE, submittedDonorId, submittedProjectId, creationRelease);

    // Verify
    assertThat(donorId).isEqualTo("DO1");
  }

  @Test
  public void test_findId_non_existing_idempotence() {
    // Setup
    String submittedDonorId = "donor1";
    String submittedProjectId = "project1";
    String creationRelease = "release1";

    // Execute and verify
    assertThat(repository.findId(CREATE, submittedDonorId, submittedProjectId, creationRelease)).isEqualTo("DO1");
    assertThat(repository.findId(CREATE, submittedDonorId, submittedProjectId, creationRelease)).isEqualTo("DO1");
  }

  @Test
  public void test_findId_existing_idempotence() {
    // Setup
    String submittedDonorId = "donor1";
    String submittedProjectId = "project1";
    String creationRelease = "release1";
    insertDonor(submittedDonorId, submittedProjectId, creationRelease);

    // Execute and verify
    assertThat(repository.findId(CREATE, submittedDonorId, submittedProjectId, creationRelease)).isEqualTo("DO1");
    assertThat(repository.findId(CREATE, submittedDonorId, submittedProjectId, creationRelease)).isEqualTo("DO1");
  }

  @Test
  public void test_findId_increment() {
    // Execute and verify
    assertThat(repository.findId(CREATE, "donor1", "project1", "release1")).isEqualTo("DO1");
    assertThat(repository.findId(CREATE, "donor2", "project1", "release1")).isEqualTo("DO2");
    assertThat(repository.findId(CREATE, "donor3", "project1", "release1")).isEqualTo("DO3");
  }

  @Test
  @Ignore
  public void concurrencyTest() throws InterruptedException, BrokenBarrierException, ExecutionException {
    int nClients = 4;
    final int nReqs = 10 * 1000;
    final CyclicBarrier barrier = new CyclicBarrier(nClients + 1);
    final ListeningExecutorService pool =
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(nClients));

    class client implements Callable<List<String>> {

      @Override
      public List<String> call() throws Exception {
        List<String> ids = newArrayListWithCapacity(nReqs);
        barrier.await();

        for (int i = 0; i < nReqs; ++i) {
          ids.add(repository.findId(CREATE, String.valueOf(i), String.valueOf(i)));
        }
        return ids;
      }
    }

    List<ListenableFuture<List<String>>> futures = newArrayListWithCapacity(nClients);
    for (int i = 0; i < nClients; ++i) {
      futures.add(pool.submit(new client()));
    }
    // Wait for all threads to get ready
    barrier.await();

    final ListenableFuture<List<List<String>>> futureOfIds = Futures.allAsList(futures);
    final ListenableFuture<Set<String>> uniqueIdsFuture =
        Futures.transform(futureOfIds, new Function<List<List<String>>, Set<String>>() {

          @Override
          public Set<String> apply(List<List<String>> results) {
            Set<String> total = Sets.newHashSetWithExpectedSize(nReqs);
            for (List<String> result : results) {
              total.addAll(result);
            }
            return total;
          }
        });

    Set<String> result = uniqueIdsFuture.get();
    assertThat(result).hasSize(nReqs);
    List<String> orderedResult = newArrayList(result);
    Collections.sort(orderedResult, new Comparator<String>() {

      @Override
      public int compare(String o1, String o2) {
        long l1 = Long.parseLong(removeStart(o1, repository.getPrefix()));
        long l2 = Long.parseLong(removeStart(o2, repository.getPrefix()));
        return Longs.compare(l1, l2);
      }
    });

    long prevId = 0;
    long nSkips = 0;
    int nGaps = 0;
    for (String id : orderedResult) {
      long curId = Long.parseLong(removeStart(id, repository.getPrefix()));
      prevId++;
      if (prevId != curId) {
        nSkips = nSkips + (curId - prevId);
        nGaps++;
      }
      prevId = curId;
    }

    System.out.println("Total gaps: " + nGaps);
    System.out.println("Total skip ids: " + nSkips);
  }

  @Test
  @Ignore
  public void basicInsertPerfTest() throws InterruptedException, BrokenBarrierException, ExecutionException {
    int nClients = 10;
    final int nReqs = 10 * 1000;
    final CyclicBarrier barrier = new CyclicBarrier(nClients + 1);
    final ListeningExecutorService pool =
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(nClients));

    class client implements Callable<List<String>> {

      private final int multiplier;

      public client(int multiplier) {
        this.multiplier = multiplier;
      }

      @Override
      public List<String> call() throws Exception {
        List<String> ids = newArrayListWithCapacity(nReqs);
        int start = multiplier * nReqs;
        int end = start + nReqs;
        barrier.await();
        for (int i = start; i < end; ++i) {
          ids.add(repository.findId(CREATE, String.valueOf(i), String.valueOf(i)));
        }
        return ids;
      }
    }

    List<ListenableFuture<List<String>>> futures = newArrayListWithCapacity(nClients);
    for (int i = 0; i < nClients; ++i) {
      futures.add(pool.submit(new client(i)));
    }
    // Wait for all threads to get ready
    final ListenableFuture<List<List<String>>> futureOfIds = Futures.allAsList(futures);
    barrier.await();
    long start = System.currentTimeMillis();
    futureOfIds.get();
    long end = System.currentTimeMillis();
    System.out.println("Througput (#id/second): " + (nReqs * nClients * 1000 / (end - start)));
  }

  @Test
  @Ignore
  public void basicReadPerfTest() throws InterruptedException, BrokenBarrierException, ExecutionException {
    int nClients = 10;
    final int nReqs = 10 * 1000;

    // insert
    for (int i = 0; i < nReqs; ++i) {
      repository.findId(CREATE, String.valueOf(i), String.valueOf(i));
    }

    final CyclicBarrier barrier = new CyclicBarrier(nClients + 1);
    final ListeningExecutorService pool =
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(nClients));

    class client implements Callable<List<String>> {

      @Override
      public List<String> call() throws Exception {
        List<String> ids = newArrayListWithCapacity(nReqs);
        barrier.await();
        for (int i = 0; i < nReqs; ++i) {
          ids.add(repository.findId(CREATE, String.valueOf(i), String.valueOf(i)));
        }
        return ids;
      }
    }

    List<ListenableFuture<List<String>>> futures = newArrayListWithCapacity(nClients);
    for (int i = 0; i < nClients; ++i) {
      futures.add(pool.submit(new client()));
    }
    // Wait for all threads to get ready
    final ListenableFuture<List<List<String>>> futureOfIds = Futures.allAsList(futures);
    barrier.await();
    long start = System.currentTimeMillis();
    futureOfIds.get();
    long end = System.currentTimeMillis();
    System.out.println("Througput (#id/second): " + (nReqs * nClients * 1000 / (end - start)));
  }

  void insertDonor(final String submittedDonorId, final String submittedProjectId, final String creationRelease) {
    insert("donor_ids",
        of("donor_id", submittedDonorId, "project_id", submittedProjectId, "creation_release", creationRelease));
  }

  String getCreationReleaseFromDonor(final int id) {
    return getCreationRelease("donor_ids", id);
  }

}
