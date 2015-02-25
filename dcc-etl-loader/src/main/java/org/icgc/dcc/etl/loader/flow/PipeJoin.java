/*
 * Copyright (c) 2013 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.loader.flow;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import static org.icgc.dcc.etl.loader.flow.LoaderFields.prefixedFields;

import java.util.List;

import lombok.NonNull;
import lombok.ToString;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.common.core.model.SubmissionModel;
import org.icgc.dcc.etl.loader.cascading.AsList;
import org.icgc.dcc.common.hadoop.cascading.SubAssemblies.NullReplacer.EmptyTupleNullReplacer;

import cascading.pipe.CoGroup;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Discard;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.Joiner;
import cascading.pipe.joiner.RightJoin;
import cascading.tuple.Fields;

import com.google.common.base.Optional;

/**
 * Only used for nesting (or not) sides + join, no more "reversing".
 * <p>
 * Make sure that the lhs is always the biggest.
 */
@ToString
@Slf4j
public class PipeJoin extends SubAssembly {

  private static final InnerJoin INNER_JOIN = new InnerJoin();
  private static final RightJoin RIGHT_JOIN = new RightJoin();

  private final FileType referencingFileType;
  private final Pipe referencingPipe;
  private final List<String> fks;

  private final FileType referencedFileType;
  private final Pipe referencedPipe;
  private final List<String> pks;

  private final boolean innerJoin;

  private final Optional<String> nestingFieldName;

  public PipeJoin(
      @NonNull final FileType referencingFileType,
      @NonNull final Pipe referencingPipe,
      @NonNull final List<String> fks,

      @NonNull final FileType referencedFileType,
      @NonNull final Pipe referencedPipe,
      @NonNull final List<String> pks,

      final boolean innerJoin,
      @NonNull final Optional<String> nestingFieldName) {
    checkArgument(!pks.isEmpty() && pks.size() == fks.size(),
        "Invalid PKs/FKs combination: ('%s', '%s')", pks, fks);

    this.referencingFileType = referencingFileType;
    this.referencingPipe = referencingPipe;
    this.fks = fks;

    this.referencedFileType = referencedFileType;
    this.referencedPipe = referencedPipe;
    this.pks = pks;

    this.innerJoin = innerJoin;
    this.nestingFieldName = nestingFieldName;

    setTails(join());
  }

  private Pipe join() {
    val lhsJoinFields = prefixedFields(referencingFileType, fks);
    val rhsJoinFields = prefixedFields(referencedFileType, pks);
    val joiner = getJoiner();
    val doNesting = nestingFieldName.isPresent();
    checkState(
        isInnerJoin(joiner) || // Either this is an inner join
            doNesting, // Or an right join with nesting of the lhs under the rhs
        describeRelation());

    val actualLhsPipe = doNesting ?
        // always except when joining on meta schema
        nestArray(referencingPipe, lhsJoinFields, getNestingField()) :
        referencingPipe;

    // Perform actual join
    val hashJoinFriendly = isHashJoinFriendly(referencedFileType);
    if (hashJoinFriendly) {
      log.info("Performing hash join with '{}' on the RHS", referencedFileType);
    }
    Pipe join = hashJoinFriendly ?
        new HashJoin(actualLhsPipe, lhsJoinFields, referencedPipe, rhsJoinFields, joiner) :
        new CoGroup(actualLhsPipe, lhsJoinFields, referencedPipe, rhsJoinFields, joiner);

    if (doNesting) {
      // Discard join field on the nested side (redundant after the join)
    }
    join = new Discard(join, doNesting ? lhsJoinFields : rhsJoinFields);

    return doNesting && !isInnerJoin(joiner) ?
        new EmptyTupleNullReplacer(getNestingField(), join) :
        join;
  }

  /**
   * TODO: move to {@link SubmissionModel}?
   */
  private String describeRelation() {
    return String.format("'%s': '%s' ('%s') -> '%s' ('%s')",
        getJoiner(), referencingFileType, fks, referencedFileType, pks);
  }

  /**
   * Prior to joining, nest all the records that share the same FK under one tuple.
   * <p>
   * The tuples coming out of this method contain:<br/>
   * - the sub-fileschema join fields<br/>
   * - a field containing a nesting of the values and named after the sub-fileschema<br/>
   */
  private Pipe nestArray(Pipe subFileSchemaPipe, Fields subFileSchemaJoinFields, Fields nestField) {

    // Group by the sub-file-schema join fields (e.g. ssm_s.analysis_id+ssm_s.analyzed_sample_id+ssm_s.mutation_id)
    subFileSchemaPipe = new GroupBy(subFileSchemaPipe, subFileSchemaJoinFields);

    // Transform elements of the group into a Tuple (acting as list) and insert it as a new field
    subFileSchemaPipe = new Every(subFileSchemaPipe, new AsList(nestField, subFileSchemaJoinFields));

    return subFileSchemaPipe;
  }

  private Fields getNestingField() {
    return new Fields(nestingFieldName.get());
  }

  /**
   * Returns joiner: inner if the underlying relation is bidirectional/surjective, right join otherwise.
   */
  private Joiner getJoiner() {
    return innerJoin ? INNER_JOIN : RIGHT_JOIN;
  }

  private static boolean isHashJoinFriendly(FileType referencedFileType) {
    return referencedFileType.getDataType().isClinicalType() ||
        referencedFileType.getSubType().isMetaSubType();
  }

  private static boolean isInnerJoin(Joiner joiner) {
    return joiner == INNER_JOIN;
  }

}
