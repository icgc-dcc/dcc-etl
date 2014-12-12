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

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

import java.util.Arrays;

import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;

public abstract class BaseRepository {

  /**
   * The maximum number of times to try to resolve an id.
   */
  private static final int MAX_ATTEMPTS = 5;

  /**
   * Template method to find or insert the id associated with the supplied business {@code keys}.
   * 
   * @param keys - the business key
   * @return the id
   */
  String findId(String... keys) {
    for (String key : keys) {
      if (key == null) {
        throw new BadRequestException(format("Business key value is null for keys '%s'", formatKeys(keys)));
      }
    }

    // Resolve the "internal" representation of the id
    Long id = resolveId(keys);

    // Format for public consumption
    return formatId(id);
  }

  private Long resolveId(String... keys) {
    // Try to find the existing key
    Long id = getId(keys);

    int attempts = 0;
    while (true) {
      boolean exists = id != null;
      if (exists) {
        // Resolved
        break;
      }

      // Bound the number of attempts
      attempts++;
      checkState(attempts < MAX_ATTEMPTS, "Could not acquire id for keys '%s' in %s attempts. Aborting.",
          formatKeys(keys), attempts);

      try {
        // Newly discovered key, so create
        id = insertId(keys);
      } catch (UnableToExecuteStatementException e) {
        // Most likely a race condition due to concurrent inserts, probably caused by a duplicate
        // key exception. However, there is no definitive way of determining if this is true so we
        // assume it. The thinking is that we will eventually resolve the value within a bounded
        // number of attempts, and if not we throw after crossing the threshold.
        id = getId(keys);
      }
    }

    return id;
  }

  /**
   * Resolve the existing {@code id}.
   * 
   * @param keys - the id keys
   * @return the existing id
   */
  abstract Long getId(String... keys);

  /**
   * Insert the new {@code id}.
   * 
   * @param keys - the id keys
   * @return the newly inserted id
   */
  abstract Long insertId(String... keys);

  /**
   * Get the the {@code id} prefix
   * 
   * @return the id prefix
   */
  abstract String getPrefix();

  /**
   * Used to close the connection
   */
  abstract void close();

  /**
   * Format the {@code id} value.
   * 
   * @param id - the id value
   * @return the formatted id
   */
  private String formatId(Long id) {
    return format("%s%s", getPrefix(), id);
  }

  /**
   * Formats the business {@code keys}.
   * 
   * @param keys - the business keys
   * @return the formatted value
   */
  private String formatKeys(String... keys) {
    return Arrays.toString(keys);
  }

}
