/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
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
package org.icgc.dcc.etl.indexer.transform;

import static com.google.common.base.Objects.firstNonNull;
import static org.icgc.dcc.common.core.util.Separators.EMPTY_STRING;

import java.util.Collection;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl.indexer.core.Document;
import org.icgc.dcc.etl.indexer.core.DocumentContext;
import org.icgc.dcc.etl.indexer.core.DocumentTransform;
import org.icgc.dcc.etl.indexer.model.DocumentType;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;

@Slf4j
public class DrugTextDocumentTransform implements DocumentTransform {

  @Override
  public Document transformDocument(@NonNull ObjectNode drug, @NonNull DocumentContext context) {
    val id = drug.remove("zinc_id").textValue();
    enrichDrug(id, drug);

    log.debug("[{}] Processsing drug: {}", id, drug);
    transformAtcCodes(drug);
    log.debug("[{}] Transformed atc_codes: {}", id, drug);
    transformTrials(drug);
    log.debug("[{}] Transformed trials: {}", id, drug);
    transformExternalReferences(drug);
    log.debug("[{}] Transformed external references. Final document: {}", id, drug);

    return new Document(DocumentType.DRUG_TEXT_TYPE, id, drug);
  }

  private static void enrichDrug(String id, ObjectNode drug) {
    drug.put("id", id);
    drug.put("type", "compound");
  }

  private static void transformAtcCodes(ObjectNode drug) {
    val atcCodesCode = Sets.<String> newHashSet();
    val atcCodesDescription = Sets.<String> newHashSet();
    val atcLevel5Codes = Sets.<String> newHashSet();

    val atcCodes = drug.path("atc_codes");
    if (atcCodes.isArray()) {
      for (val atcCodeEntity : atcCodes) {
        addStringValue(atcCodeEntity, "code", atcCodesCode);
        addStringValue(atcCodeEntity, "description", atcCodesDescription);
        addStringValue(atcCodeEntity, "atc_level5_codes", atcLevel5Codes);
      }
    }

    drug.remove("atc_codes");
    addStringArray(drug, "atc_codes_code", atcCodesCode);
    addStringArray(drug, "atc_codes_description", atcCodesDescription);
    addStringArray(drug, "atc_level5_codes", atcLevel5Codes);
  }

  private static void transformTrials(ObjectNode drug) {
    val trialDescriptions = Sets.<String> newHashSet();
    val trialConditionNames = Sets.<String> newHashSet();

    val trials = drug.path("trials");
    if (trials.isArray()) {
      for (val trial : trials) {
        addStringValue(trial, "description", trialDescriptions);
        trialConditionNames.addAll(getConditionNames(trial.path("conditions")));
      }
    }

    drug.remove("trials");
    addStringArray(drug, "trials_description", trialDescriptions);
    addStringArray(drug, "trials_conditions_name", trialConditionNames);
  }

  private static Collection<String> getConditionNames(JsonNode conditions) {
    val conditionsNames = Sets.<String> newHashSet();
    if (conditions.isArray()) {
      for (val condition : conditions) {
        addStringValue(condition, "name", conditionsNames);
      }
    }

    return conditionsNames;
  }

  private static void transformExternalReferences(ObjectNode drug) {
    val externalReferences = drug.path("external_references");
    if (!externalReferences.isMissingNode()) {
      val drugbank = externalReferences.path("drugbank");
      drug.put("external_references_drugbank", drugbank.deepCopy());

      val chembl = externalReferences.path("chembl");
      drug.put("external_references_chembl", chembl.deepCopy());
    }
    drug.remove("external_references");
  }

  private static void addStringArray(ObjectNode document, String arrayName, Iterable<String> values) {
    val target = document.withArray(arrayName);
    for (val value : values) {
      target.add(value);
    }
  }

  private static void addStringValue(JsonNode node, String fieldName, Collection<String> target) {
    val value = getText(node, fieldName);
    if (!EMPTY_STRING.equals(value)) {
      target.add(value);
    }
  }

  private static String getText(JsonNode node, String fieldName) {
    val value = node.path(fieldName);

    return value.isMissingNode() ? EMPTY_STRING : firstNonNull(value.textValue(), EMPTY_STRING);
  }

}
