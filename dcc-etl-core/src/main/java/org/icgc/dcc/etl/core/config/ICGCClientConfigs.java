/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl.core.config;

import static java.lang.Boolean.valueOf;

import java.io.File;

import lombok.NonNull;

import org.icgc.dcc.common.client.api.ICGCClientConfig;

public class ICGCClientConfigs {

  @NonNull
  public static ICGCClientConfig createICGCConfig(String etlConfigFile) {
    return createICGCConfig(EtlConfigFile.read(new File(etlConfigFile)));
  }

  @NonNull
  public static ICGCClientConfig createICGCConfig(EtlConfig config) {
    return ICGCClientConfig.builder()
        .cgpServiceUrl(config.getIcgc().get("cgpServiceUrl"))
        .consumerKey(config.getIcgc().get("consumerKey"))
        .consumerSecret(config.getIcgc().get("consumerSecret"))
        .accessToken(config.getIcgc().get("accessToken"))
        .accessSecret(config.getIcgc().get("accessSecret"))
        .strictSSLCertificates(valueOf(config.getIcgc().get("strictSSLCertificates")))
        .requestLoggingEnabled(valueOf(config.getIcgc().get("requestLoggingEnabled")))
        .build();
  }

}
