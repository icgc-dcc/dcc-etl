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

import org.icgc.dcc.etl.identifier.repository.SampleRepository;
import org.junit.Test;

public class SampleRepositoryTest extends BaseRepositoryTest<SampleRepository> {

  @Test
  public void test_findId_non_existing() {
    // Execute
    String id = repository.findId("1", "1", "1");

    // Verify
    assertThat(id).isEqualTo("SA1");
  }

  @Test
  public void test_findId_different_release() {
    // Execute
    String id = repository.findId("1", "1", "1");

    // Verify
    assertThat(id).isEqualTo("SA1");

    // Execute
    id = repository.findId("1", "1", "2");

    // Verify
    assertThat(id).isEqualTo("SA1");
    assertThat(getCreationRelease("sample_ids", 1)).isEqualTo("1");
  }

  @Test
  public void test_findId_increment() {
    // Execute and verify
    assertThat(repository.findId("1", "1", "1")).isEqualTo("SA1");
    assertThat(repository.findId("1", "2", "1")).isEqualTo("SA2");
    assertThat(repository.findId("1", "3", "1")).isEqualTo("SA3");
  }

}
