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
package org.icgc.dcc.etl.identifier.resource;

import static com.sun.jersey.api.client.ClientResponse.Status.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import lombok.val;

import org.icgc.dcc.etl.identifier.repository.BadRequestException;
import org.icgc.dcc.etl.identifier.repository.MutationRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.sun.jersey.api.client.ClientResponse;
import com.yammer.dropwizard.testing.ResourceTest;

@RunWith(MockitoJUnitRunner.class)
public class MutationResourceTest extends ResourceTest {

  static final boolean CREATE = true;

  @Mock
  private MutationRepository repository;

  @Override
  protected final void setUpResources() {
    addResource(new MutationResource(repository));
  }

  @Test
  public void test_findId() {
    // Setup
    String chromosome = "chr1";
    String chromosomeStart = "1";
    String chromosomeEnd = "2";
    String mutation = "C>T";
    String mutationType = "single base substitution";
    String assemblyVersion = "GRCh37";
    String release = "release1";
    String mutationId = "MU1";

    when(
        repository.findId(CREATE, chromosome, chromosomeStart, chromosomeEnd, mutationType, mutation, assemblyVersion,
            release))
        .thenReturn(mutationId);

    // Execute
    val response = client()
        .resource("/mutation/id")
        .queryParam("chromosome", chromosome)
        .queryParam("chromosomeStart", chromosomeStart)
        .queryParam("chromosomeEnd", chromosomeEnd)
        .queryParam("mutation", mutation)
        .queryParam("mutationType", mutationType)
        .queryParam("assemblyVersion", assemblyVersion)
        .queryParam("release", release)
        .queryParam("create", Boolean.toString(CREATE))
        .get(ClientResponse.class);

    // Verify
    verify(repository).findId(CREATE, chromosome, chromosomeStart, chromosomeEnd, mutationType, mutation,
        assemblyVersion,
        release);
    assertThat(response.getClientResponseStatus()).isEqualTo(OK);
  }

  @Test(expected = BadRequestException.class)
  public void test_findId_null() {
    // Setup

    String chromosome = "chr1";
    String chromosomeStart = null;
    String chromosomeEnd = "2";
    String mutation = "C>T";
    String mutationType = "single base substitution";
    String assemblyVersion = "GRCh37";
    String release = "release1";

    when(
        repository.findId(CREATE, chromosome, chromosomeStart, chromosomeEnd, mutationType, mutation, assemblyVersion,
            release))
        .thenThrow(new BadRequestException(""));
    // Execute
    client()
        .resource("/mutation/id")
        .queryParam("chromosome", chromosome)
        .queryParam("chromosomeEnd", chromosomeEnd)
        .queryParam("mutation", mutation)
        .queryParam("mutationType", mutationType)
        .queryParam("assemblyVersion", assemblyVersion)
        .queryParam("release", release)
        .queryParam("create", Boolean.toString(CREATE))
        .get(ClientResponse.class);
  }
}
