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
package org.icgc.dcc.etl.loader.core;

import java.io.Serializable;

import lombok.Value;

import org.icgc.dcc.common.core.model.SubmissionModel;

/**
 * "Encapsulated Context Object" class that insulates and decouples the loader subsystem from validator abstractions.
 * This is very similar in purpose to Hadoop's new API "Context Object for Mapper and Reducer".
 * <p>
 * Note that there still remain some accessors that should be removed as they violate the "Law of Demeter". In this
 * sense, this should be considered a "Parameter Object" transitioning to a "Encapsulated Context Object".
 * 
 * @see http://www.two-sdg.demon.co.uk/curbralan/papers/europlop/ContextEncapsulation.pdf
 * @see http://www.allankelly.net/static/patterns/encapsulatecontext.pdf
 */
@Value
public class LoaderContext implements Serializable {

  SubmissionModel submissionModel;
  String identifierClientClassName;
  String identifierUri;
  String releaseName;
  String identifierAuthToken;
  boolean filterAllControlled;

}
