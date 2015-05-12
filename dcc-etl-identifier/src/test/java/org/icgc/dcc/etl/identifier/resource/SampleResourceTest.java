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

import org.icgc.dcc.etl.identifier.repository.SampleRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.sun.jersey.api.client.ClientResponse;
import com.yammer.dropwizard.testing.ResourceTest;

@RunWith(MockitoJUnitRunner.class)
public class SampleResourceTest extends ResourceTest {

  static final boolean CREATE = true;

  @Mock
  private SampleRepository repository;

  @Override
  protected final void setUpResources() {
    addResource(new SampleResource(repository));
  }

  @Test
  public void test_findId() {
    // Setup
    String sampleId = "SA1";
    String submittedSampleId = "sample1";
    String submittedProjectId = "project1";
    String release = "release1";
    when(repository.findId(CREATE, submittedSampleId, submittedProjectId, release)).thenReturn(sampleId);

    // Execute
    val response = client()
        .resource("/sample/id")
        .queryParam("submittedSampleId", submittedSampleId)
        .queryParam("submittedProjectId", submittedProjectId)
        .queryParam("release", release)
        .queryParam("create", Boolean.toString(CREATE))
        .get(ClientResponse.class);

    // Verify
    verify(repository).findId(CREATE, submittedSampleId, submittedProjectId, release);
    assertThat(response.getClientResponseStatus()).isEqualTo(OK);
  }
}
