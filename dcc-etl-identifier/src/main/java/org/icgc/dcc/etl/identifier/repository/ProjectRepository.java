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

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;

public abstract class ProjectRepository extends BaseRepository {

  public String findId(boolean create, String submittedProjectId, String release) {
    return super.findId(create, submittedProjectId, release);
  }

  /**
   * Template method extension point.
   */
  @Override
  Long getId(String... keys) {
    return getProjectId(keys[0] /* projectId */);
  }

  /**
   * Template method extension point.
   */
  @Override
  Long insertId(String... keys) {
    return insertProjectId(
        keys[0], /* projectId */
        keys[1] /* creationRelease */);
  }

  /**
   * Template method extension point.
   */
  @Override
  String getPrefix() {
    return "PR";
  }

  /**
   * JDBI instrumented.
   */
  @SqlQuery("sql/project-id-select")
  abstract Long getProjectId(
      @Bind("projectId") String projectId);

  /**
   * JDBI instrumented.
   */
  @SqlUpdate("sql/project-id-insert")
  @GetGeneratedKeys
  abstract Long insertProjectId(
      @Bind("projectId") String projectId,

      @Bind("creationRelease") String creationRlease);
}
