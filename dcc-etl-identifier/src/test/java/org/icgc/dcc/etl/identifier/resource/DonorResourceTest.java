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
import org.icgc.dcc.etl.identifier.repository.DonorRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.sun.jersey.api.client.ClientResponse;
import com.yammer.dropwizard.testing.ResourceTest;

@RunWith(MockitoJUnitRunner.class)
public class DonorResourceTest extends ResourceTest {

  static final boolean CREATE = true;

  @Mock
  private DonorRepository repository;

  @Override
  protected final void setUpResources() {
    addResource(new DonorResource(repository));
  }

  @Test
  public void test_findId() {
    // Setup
    String submittedDonorId = "donor1";
    String submittedProjectId = "project1";
    String release = "release1";
    String donorId = "DO1";
    when(repository.findId(CREATE, submittedDonorId, submittedProjectId, release)).thenReturn(donorId);

    // Execute
    val response = client()
        .resource("/donor/id")
        .queryParam("submittedDonorId", submittedDonorId)
        .queryParam("submittedProjectId", submittedProjectId)
        .queryParam("release", release)
        .queryParam("create", Boolean.toString(CREATE))
        .get(ClientResponse.class);

    // Verify
    verify(repository).findId(CREATE, submittedDonorId, submittedProjectId, release);
    assertThat(response.getClientResponseStatus()).isEqualTo(OK);
  }

  @Test(expected = BadRequestException.class)
  public void test_findId_null() {
    // Setup
    String submittedDonorId = "donor1";
    String submittedProjectId = null;
    String release = "release1";

    when(repository.findId(CREATE, submittedDonorId, submittedProjectId, release)).thenThrow(
        new BadRequestException(""));

    // Execute
    client()
        .resource("/donor/id")
        .queryParam("submittedDonorId", submittedDonorId)
        .queryParam("release", release)
        .queryParam("create", Boolean.toString(CREATE))
        .get(ClientResponse.class);
  }

}
