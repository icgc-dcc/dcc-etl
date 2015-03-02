/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
 *                                                                                                               
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with                                  
 * this program. If not, see <http://www.gnu.org/licenses/>.                                                     
 *                                                                                                               
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY                           
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES                          
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT                           
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,                                
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED                          
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;                               
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER                              
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN                         
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.etl.db.importer.pathway;

import static org.icgc.dcc.etl.db.importer.util.Importers.getLocalGenesBsonUri;
import static org.icgc.dcc.etl.db.importer.util.Importers.getLocalMongoClientUri;
import static org.icgc.dcc.etl.db.importer.util.Importers.getLocalReactomeHierarchyUri;
import static org.icgc.dcc.etl.db.importer.util.Importers.getLocalReactomeSummationUri;
import static org.icgc.dcc.etl.db.importer.util.Importers.getLocalReactomeUniprotUri;
import lombok.SneakyThrows;
import lombok.val;

import org.icgc.dcc.etl.db.importer.gene.GeneImporter;
import org.junit.Before;
import org.junit.Test;

public class PathwayImporterTest {

  @Before
  public void setUp() {
    // Create a fresh copy of the entire gene model
    new GeneImporter(
        getLocalMongoClientUri("dcc-genome"),
        getLocalGenesBsonUri())
        .execute();
  }

  @Test
  @SneakyThrows
  public void testExecute() {
    val pathwayImporter =
        new PathwayImporter(
            getLocalReactomeUniprotUri(),
            getLocalReactomeSummationUri(),
            getLocalReactomeHierarchyUri(),
            getLocalMongoClientUri("dcc-genome"));

    pathwayImporter.execute();
  }

}
