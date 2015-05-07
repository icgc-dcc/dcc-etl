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

public abstract class MutationRepository extends BaseRepository {

  public String findId(boolean create, String chromosome, String chromosomeStart, String chromosomeEnd,
      String mutationType, String mutation, String assemblyVersion, String release) {
    return super.findId(create, chromosome, chromosomeStart, chromosomeEnd, mutationType, mutation, assemblyVersion,
        release);
  }

  /**
   * Template method extension point.
   */
  @Override
  Long getId(String... keys) {
    return getMutationId(
        keys[0], // chromosome
        keys[1], // chromosomeStart
        keys[2], // chromosomeEnd
        keys[3], // mutationType
        keys[4], // mutation
        keys[5] // assemblyVersion
    );
  }

  /**
   * Template method extension point.
   */
  @Override
  Long insertId(String... keys) {
    return insertMutationId(
        keys[0], // chromosome
        keys[1], // chromosomeStart
        keys[2], // chromosomeEnd
        keys[3], // mutationType
        keys[4], // mutation
        keys[5], // assemblyVersion
        keys[6] // creation_release
    );
  }

  /**
   * Template method extension point.
   */
  @Override
  String getPrefix() {
    return "MU";
  }

  /**
   * JDBI instrumented.
   */
  @SqlQuery("sql/mutation-id-select")
  abstract Long getMutationId(
      @Bind("chromosome") String chromosome,

      @Bind("chromosomeStart") String chromosomeStart,

      @Bind("chromosomeEnd") String chromosomeEnd,

      @Bind("mutationType") String mutationType,

      @Bind("mutation") String mutation,

      @Bind("assemblyVersion") String assemblyVersion);

  /**
   * JDBI instrumented.
   */
  @SqlUpdate("sql/mutation-id-insert")
  @GetGeneratedKeys
  abstract Long insertMutationId(
      @Bind("chromosome") String chromosome,

      @Bind("chromosomeStart") String chromosomeStart,

      @Bind("chromosomeEnd") String chromosomeEnd,

      @Bind("mutationType") String mutationType,

      @Bind("mutation") String mutation,

      @Bind("assemblyVersion") String assemblyVersion,

      @Bind("creationRelease") String creationRlease);
}