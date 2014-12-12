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

import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static org.h2.jdbcx.JdbcConnectionPool.create;

import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.Map;

import org.junit.Before;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.logging.PrintStreamLog;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.StringMapper;

import com.google.common.base.Joiner;

public class BaseRepositoryTest<T> {

  private static final Joiner JOINER = Joiner.on(',');

  /**
   * Database access.
   */

  private static final DBI dbi = new DBI(create("jdbc:h2:mem;MODE=PostgreSQL", "username", "password"));

  /**
   * Class under test (CUT).
   */
  final T repository = dbi.onDemand(getRepositoryClass());

  @Before
  public void setUp() {
    // Log sql statements
    dbi.setSQLLog(new PrintStreamLog());

    // Initialize the schema
    dbi.withHandle(new HandleCallback<int[]>() {

      @Override
      public int[] withHandle(Handle h) throws Exception {
        return h.createScript("sql/schema.sql").execute();
      }
    });
  }

  /**
   * Dynamically insert a record.
   */
  void insert(final String tableName, final Map<String, String> record) {
    dbi.withHandle(new HandleCallback<Integer>() {

      @Override
      public Integer withHandle(Handle h) throws Exception {
        return h.insert(sql(tableName, record.keySet()), record.values().toArray());
      }

      private String sql(final String tableName, final Collection<String> columnNames) {
        String columns = JOINER.join(columnNames);
        String placeholders = JOINER.join(nCopies(columnNames.size(), "?"));

        return format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, placeholders);
      }
    });
  }

  String getCreationRelease(final String tableName, final int id) {
    Handle handle = dbi.open();
    String creationRelease = handle.createQuery("select creation_release from " + tableName + " where id = :id")
        .bind("id", id)
        .map(StringMapper.FIRST)
        .first();
    handle.close();
    return creationRelease;
  }

  /**
   * Get reified generic super class.
   */
  @SuppressWarnings("unchecked")
  private Class<T> getRepositoryClass() {
    return (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
  }

}
