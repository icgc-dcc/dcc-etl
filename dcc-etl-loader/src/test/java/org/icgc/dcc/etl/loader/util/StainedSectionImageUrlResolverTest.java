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
package org.icgc.dcc.etl.loader.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import lombok.val;

import org.junit.Ignore;
import org.junit.Test;

public class StainedSectionImageUrlResolverTest {

  @Test
  public void testResolveUrlWithValidation() {
    val validate = true;
    val resolver = new StainedSectionImageUrlResolver(validate);

    val validSpecimenId = "TCGA-OL-A97C-01Z";
    val url = resolver.resolveUrl(validSpecimenId);

    assertThat(url).isEqualTo("http://cancer.digitalslidearchive.net/index_mskcc.php?slide_name=" + validSpecimenId);
  }

  @Test
  public void testResolveUrlWithoutValidation() {
    val validate = false;
    val resolver = new StainedSectionImageUrlResolver(validate);

    val invalidSpecimenId = "SP123";
    val url = resolver.resolveUrl(invalidSpecimenId);

    assertThat(url).isEqualTo("http://cancer.digitalslidearchive.net/index_mskcc.php?slide_name=" + invalidSpecimenId);
  }

  @Test
  public void testResolveUrlWithValidationInvalidSpecimenId() {
    val validate = true;
    val resolver = new StainedSectionImageUrlResolver(validate);

    val validSpecimenId = "dummy";
    val url = resolver.resolveUrl(validSpecimenId);

    assertThat(url).isEqualTo(StainedSectionImageUrlResolver.NO_MATCH_REPLACEMENT_VALUE);
  }

  @Test
  @Ignore("Useful for generating all valid urls")
  public void testResolveUrlAllValid() throws FileNotFoundException, IOException {
    val validate = true;
    val resolver = new StainedSectionImageUrlResolver(validate);

    val urls = resolver.resolveUrls();

    val properties = new Properties();
    properties.putAll(urls);
    properties.store(new FileOutputStream("target/stained-image-urls.properties"), "Stained image urls");
  }

}
