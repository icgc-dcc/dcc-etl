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
package org.icgc.dcc.etl.annotator.cascading;

import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.etl.annotator.config.SnpEffProperties;
import org.icgc.dcc.etl.annotator.model.AnnotatedFileType;

import cascading.pipe.Each;
import cascading.pipe.SubAssembly;

/**
 * {@link SubAssembly} implementation that is responsible for executing a project annotation flow.
 */
public class ProjectAnnotationSubAssembly extends SubAssembly {

  public ProjectAnnotationSubAssembly(@NonNull String projectName, @NonNull AnnotatedFileType fileType,
      @NonNull SnpEffProperties properties) {
    val name = "annotation-snpeff-" + projectName;
    val snpEff = new SnpEffFunction(properties, fileType);
    val pipe = new Each(name, snpEff);

    setTails(pipe);
  }

}
