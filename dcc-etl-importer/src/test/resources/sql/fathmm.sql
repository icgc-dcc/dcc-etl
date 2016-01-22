-- Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
--
-- This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
-- You should have received a copy of the GNU General Public License along with
-- this program. If not, see <http://www.gnu.org/licenses/>.
--
-- THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
-- EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
-- OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
-- SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
-- INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
-- TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
-- OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
-- IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
-- ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


DROP TABLE if exists "DOMAINS";
CREATE TABLE "DOMAINS" (
    id integer NOT NULL,
    hmm character(15) NOT NULL,
    score double precision NOT NULL,
    seq_begin integer NOT NULL,
    seq_end integer NOT NULL,
    hmm_begin integer NOT NULL,
    align text NOT NULL
);

DROP TABLE if exists "LIBRARY"; 
CREATE TABLE "LIBRARY" (
    id character(15) NOT NULL,
    accession character(30) NOT NULL,
    description text
);


DROP TABLE if exists "ONTOLOGIES";
CREATE TABLE "ONTOLOGIES" (
    id character(2) NOT NULL,
    description text NOT NULL
);

DROP TABLE if exists "PROBABILITIES";
CREATE TABLE "PROBABILITIES" (
    id character(15) NOT NULL,
    "position" integer NOT NULL,
    "A" double precision NOT NULL,
    "C" double precision NOT NULL,
    "D" double precision NOT NULL,
    "E" double precision NOT NULL,
    "F" double precision NOT NULL,
    "G" double precision NOT NULL,
    "H" double precision NOT NULL,
    "I" double precision NOT NULL,
    "K" double precision NOT NULL,
    "L" double precision NOT NULL,
    "M" double precision NOT NULL,
    "N" double precision NOT NULL,
    "P" double precision NOT NULL,
    "Q" double precision NOT NULL,
    "R" double precision NOT NULL,
    "S" double precision NOT NULL,
    "T" double precision NOT NULL,
    "V" double precision NOT NULL,
    "W" double precision NOT NULL,
    "Y" double precision NOT NULL,
    information double precision NOT NULL
);

DROP TABLE if exists "PROTEIN";
CREATE TABLE "PROTEIN" (
    id integer NOT NULL,
    name character(100) NOT NULL
);

DROP TABLE if exists "SEQUENCE";
CREATE TABLE "SEQUENCE" (
    id integer NOT NULL,
    sequence text NOT NULL
);

DROP TABLE if exists "VARIANTS";
CREATE TABLE "VARIANTS" (
    id character(25) NOT NULL,
    protein character(100) NOT NULL,
    substitution character(10) NOT NULL
);

DROP TABLE if exists "WEIGHTS";
CREATE TABLE "WEIGHTS" (
    id character(15) NOT NULL,
    type character(64) NOT NULL,
    disease double precision NOT NULL,
    other double precision NOT NULL
);

DROP TABLE if exists "DCC_CACHE";
CREATE TABLE "DCC_CACHE" (
   translation_id varchar(64) NOT NULL,
   aa_mutation varchar(64) NOT NULL,
   score varchar(16),
   prediction varchar(16)
);

ALTER TABLE "LIBRARY"
    ADD CONSTRAINT "LIBRARY_pkey" PRIMARY KEY (id);

ALTER TABLE "ONTOLOGIES"
    ADD CONSTRAINT "ONTOLOGIES_pkey" PRIMARY KEY (id);

ALTER TABLE "PROBABILITIES"
    ADD CONSTRAINT "PROBABILITIES_pkey" PRIMARY KEY (id, "position");

ALTER TABLE "PROTEIN"
    ADD CONSTRAINT "PROTEIN_pkey" PRIMARY KEY (name);


ALTER TABLE "SEQUENCE"
    ADD CONSTRAINT "SEQUENCE_pkey" PRIMARY KEY (id);

ALTER TABLE "WEIGHTS"
    ADD CONSTRAINT "WEIGHTS_pkey" PRIMARY KEY (id, type);

CREATE INDEX i1 ON "DOMAINS" (id);
CREATE INDEX i2 ON "DOMAINS" (hmm);
CREATE INDEX i3 on "DCC_CACHE" (translation_id, aa_mutation);


