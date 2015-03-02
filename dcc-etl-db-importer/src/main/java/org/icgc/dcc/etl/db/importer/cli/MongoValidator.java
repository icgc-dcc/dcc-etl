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
package org.icgc.dcc.etl.db.importer.cli;

import static java.lang.String.format;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.ParameterException;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class MongoValidator implements IValueValidator<String> {

  @Override
  public void validate(String name, String uri) throws ParameterException {
    MongoClientURI mongoUri = new MongoClientURI(uri);
    try {
      Mongo mongo = new MongoClient(mongoUri);
      try {
        // Test connectivity
        Socket socket = mongo.getMongoOptions().socketFactory.createSocket();
        socket.connect(mongo.getAddress().getSocketAddress());

        // All good
        socket.close();
      } catch (IOException ex) {
        parameterException(name, mongoUri, "is not accessible");
      } finally {
        mongo.close();
      }
    } catch (UnknownHostException e) {
      parameterException(name, mongoUri, "host IP address could not be determined.");
    }
  }

  private static void parameterException(String name, MongoClientURI mongoUri, String message)
      throws ParameterException {
    throw new ParameterException(format("Invalid option: %s: %s %s", name, mongoUri, message));
  }

}
