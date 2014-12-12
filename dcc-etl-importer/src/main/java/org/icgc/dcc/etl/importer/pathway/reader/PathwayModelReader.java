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
package org.icgc.dcc.etl.importer.pathway.reader;

import static com.google.common.base.Stopwatch.createStarted;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;

import java.io.IOException;
import java.net.URI;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.importer.pathway.core.PathwayModel;
import org.icgc.dcc.etl.importer.pathway.model.Pathway;

import com.google.common.collect.Maps;

@Slf4j
public class PathwayModelReader {

  public PathwayModel read(URI uniprotFile, URI summationFile, URI hierarchyFile) throws IOException {
    val watch = createStarted();

    log.info("Reading pathway summations...");
    val summations = new PathwaySummationReader().read(summationFile);

    log.info("Reading pathway uniprots...");
    val uniprots = new PathwayUniprotReader().read(uniprotFile);

    log.info("Reading pathway hierarchies...");
    val hierarchies = new PathwayHierarchyReader().read(hierarchyFile);

    log.info("Creating pathway model...");
    val model = PathwayModel.builder()
        .summations(summations)
        .uniprots(uniprots)
        .hierarchies(hierarchies)
        .pathways(Maps.<String, Pathway> newHashMap())
        .build();

    log.info("Updating pathway model...");
    model.update();

    log.info("Read {} pathway summations, {} uniprots and {} hierarchies in {}",
        formatCount(summations), formatCount(uniprots), formatCount(hierarchies.size()), watch);

    return model;
  }

}
