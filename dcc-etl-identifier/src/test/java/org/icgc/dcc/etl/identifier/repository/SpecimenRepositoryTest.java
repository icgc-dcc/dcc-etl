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

import static org.assertj.core.api.Assertions.assertThat;

import org.icgc.dcc.etl.identifier.repository.SpecimenRepository;
import org.junit.Test;

public class SpecimenRepositoryTest extends BaseRepositoryTest<SpecimenRepository> {

  @Test
  public void test_findId_non_existing() {
    // Setup
    String submittedSpecimenId = "specimen1";
    String submittedProjectId = "project1";
    String release = "release";

    // Execute
    String id = repository.findId(submittedSpecimenId, submittedProjectId, release);

    // Verify
    assertThat(id).isEqualTo("SP1");
  }

  @Test
  public void test_findId_different_release() {
    // Setup
    String submittedSpecimenId = "specimen1";
    String submittedProjectId = "project1";
    String release1 = "release1";
    String release2 = "release2";

    // Execute
    String id = repository.findId(submittedSpecimenId, submittedProjectId, release1);

    // Verify
    assertThat(id).isEqualTo("SP1");

    id = repository.findId(submittedSpecimenId, submittedProjectId, release2);

    assertThat(id).isEqualTo("SP1");

    assertThat(getCreationRelease("specimen_ids", 1)).isEqualTo("release1");
  }

  @Test
  public void test_findId_increment() {
    // Execute and verify
    assertThat(repository.findId("s1", "project1", "release1")).isEqualTo("SP1");
    assertThat(repository.findId("s2", "project1", "release1")).isEqualTo("SP2");
    assertThat(repository.findId("s3", "project1", "release1")).isEqualTo("SP3");
  }

}
