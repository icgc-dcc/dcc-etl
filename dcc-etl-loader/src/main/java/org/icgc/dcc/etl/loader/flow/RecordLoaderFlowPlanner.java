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
package org.icgc.dcc.etl.loader.flow;

import org.icgc.dcc.common.core.model.DataType;
import org.icgc.dcc.etl.loader.platform.LoaderPlatformStrategy;

import cascading.flow.Flow;

/**
 * Plans a {@code Flow} for a particular {@code Schema} describing how to process a {@link DataType}.
 */
public interface RecordLoaderFlowPlanner {

  /**
   * Returns a description for log messages mostly.
   */
  String getDescription();

  /**
   * Returns the name of the submission being processed.
   */
  String getSubmission();

  /**
   * Returns the {@link DataType} being processed.
   */
  DataType getDataType();

  /**
   * Plans the {@link Flow} corresponding to the {@link Schema} being processed.
   */
  void plan();

  /**
   * Connects the plan created in the {@link #plan()} step.
   */
  Flow<?> connect(LoaderPlatformStrategy platformStrategy);

}
