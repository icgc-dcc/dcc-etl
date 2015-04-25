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
package org.icgc.dcc.etl.core.id;

import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class HttpIdentifierClientTest {

  private static HttpIdentifierClient idClient;

  @BeforeClass
  public static void startup() throws Exception {
    idClient = new HttpIdentifierClient("http://localhost:8080", "test-release");
  }

  @AfterClass
  public static void shutdown() {
    idClient.close();
  }

  @Test
  @Ignore
  public void basicPerfTest() throws InterruptedException, BrokenBarrierException, ExecutionException {
    int nClients = 5;
    final int nReqs = 10 * 1000;

    final CyclicBarrier barrier = new CyclicBarrier(nClients + 1);
    final ListeningExecutorService pool =
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(nClients));

    class client implements Callable<List<String>> {

      private final int id;

      public client(int id) {
        this.id = id;
      }

      @Override
      public List<String> call() throws Exception {
        List<String> ids = newArrayListWithCapacity(nReqs);
        int start = id * nReqs;
        int end = start + nReqs;
        barrier.await();
        for (int i = start; i < end; ++i) {
          ids.add(idClient.getDonorId(String.valueOf(i), String.valueOf(i)));
        }
        return ids;
      }
    }

    List<ListenableFuture<List<String>>> futures = newArrayListWithCapacity(nClients);
    for (int j = 0; j < nClients; ++j) {
      futures.add(pool.submit(new client(j)));
    }
    // Wait for all threads to get ready
    final ListenableFuture<List<List<String>>> futureOfIds = Futures.allAsList(futures);
    barrier.await();
    Stopwatch watch = Stopwatch.createStarted();
    futureOfIds.get();
    watch.stop();
    System.out.println("# Client :" + nClients + ", #Req/sec :" + (nReqs * nClients)
        / watch.elapsed(SECONDS));
  }

}
