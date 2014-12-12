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
package org.icgc.dcc.etl.core.model;

import lombok.NonNull;
import lombok.Value;

/**
 * The id that uniquely defines a particular run of the ETL pipeline. Note that attempt number is not included as this
 * would prevent continuation of a run in the event of a failure.
 * 
 * @see https://jira.oicr.on.ca/browse/DCC-2434
 */
// TODO: add support for String representation formatting and parsing.
@Value
public class JobId {

  /**
   * The name of a release.
   * <p>
   * e.g. {@code ICGC17}, {@code test17}
   */
  @NonNull
  String releaseName;

  /**
   * The patch number that indicates a data correction.
   * <p>
   * This should typically be 0, but may need to be bumped in cases such as {@code ICGC 15.1}.
   */
  int patchNumber;

  /**
   * The run number that is incremented on each full run of the ETL pipeline.
   */
  int runNumber;

}
