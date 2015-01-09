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
package org.icgc.dcc.etl.importer.fathmm.reader;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_TRANSCRIPTS;
import static org.icgc.dcc.common.core.model.ReleaseCollection.GENE_COLLECTION;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.etl.importer.util.AbstractJongoComponent;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.mongodb.MongoClientURI;

/**
 * Creates a one-to-one transcript->translation
 */
public class FathmmTranscriptReader extends AbstractJongoComponent {

  public FathmmTranscriptReader(@NonNull MongoClientURI mongoUri) {
    super(mongoUri);
  }

  public BiMap<String, String> read() {
    val geneCollection = getCollection(GENE_COLLECTION);

    val mapping = ImmutableBiMap.<String, String> builder();
    val geneNodes = geneCollection.find()
        .projection("{ 'transcripts.id': 1, 'transcripts.translation_id': 1 }")
        .as(ObjectNode.class);

    for (val geneNode : geneNodes) {
      val transcripts = geneNode.get(GENE_TRANSCRIPTS);
      if (transcripts.isArray()) {
        for (val transcript : transcripts) {
          // 1-to-1 relation
          val translationId = transcript.get("translation_id").textValue();
          val transcriptId = transcript.get("id").textValue();
          if (isNullOrEmpty(translationId) || isNullOrEmpty(transcriptId)) {
            continue;
          }

          mapping.put(transcriptId, translationId);
        }
      }
    }

    return mapping.build();
  }

}
