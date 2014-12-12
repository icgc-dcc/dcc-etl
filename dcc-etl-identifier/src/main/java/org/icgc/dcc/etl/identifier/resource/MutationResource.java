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

import static javax.ws.rs.core.MediaType.TEXT_PLAIN;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import lombok.AllArgsConstructor;
import lombok.NonNull;

import org.icgc.dcc.etl.identifier.repository.MutationRepository;

@Path("/mutation")
@Produces(TEXT_PLAIN)
@AllArgsConstructor
public class MutationResource {

  @NonNull
  private final MutationRepository repository;

  @Path("/id")
  @GET
  public String mutationId(
      @QueryParam("chromosome") String chromosome,

      @QueryParam("chromosomeStart") String chromosomeStart,

      @QueryParam("chromosomeEnd") String chromosomeEnd,

      @QueryParam("mutation") String mutation,

      @QueryParam("mutationType") String mutationType,

      @QueryParam("assemblyVersion") String assemblyVersion,

      @QueryParam("release") String release)
  {
    return repository.findId(chromosome, chromosomeStart, chromosomeEnd, mutationType, mutation, assemblyVersion,
        release);
  }

}
