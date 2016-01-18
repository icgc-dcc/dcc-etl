#!/bin/bash
#
# Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
#
# This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
# You should have received a copy of the GNU General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
# OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
# SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
# TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
# OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
# IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


run_number=${1?}
date "+%y%m%d%H%M%S"
start=$(date +%s)

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=ALL-US \
  -param donor_file=us__XX__YYY__donor__20130806.txt \
  -param specimen_file=us__XX__YYY__specimen__20130806.txt \
  -param sample_file=us__XX__YYY__sample__20130806.txt \
  -param ssm_m_file=ssm__target_all__m__20130815.txt \
  -param ssm_p_file=ssm__target_all__p__20130815.txt \
  -param ssm_s_file=ssm__target_all__s__20130815.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=BOCA-UK \
  -param donor_file=uk__02__093__donor__20130904.txt \
  -param specimen_file=uk__02__093__specimen__20130904.txt \
  -param sample_file=uk__02__093__sample__20130904.txt \
  -param ssm_m_file=ssm__uk__02__093__m__60__20130708.txt \
  -param ssm_p_file=ssm__uk__02__093__p__60__20130708.txt \
  -param ssm_s_file=ssm__uk__02__093__s__60__20130708.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=BRCA-UK \
  -param donor_file=uk__02__093__donor__20130906.txt \
  -param specimen_file=uk__02__093__specimen__20130906.txt \
  -param sample_file=uk__02__093__sample__20130906.txt \
  -param ssm_m_file=ssm__uk__02__093__m__5__20130906.txt \
  -param ssm_p_file=ssm__uk__02__093__p__5__20130906.txt \
  -param ssm_s_file=ssm__uk__02__093__s__5__20130906.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=BRCA-US \
  -param donor_file=us__02__097__donor__20130423fix999.txt \
  -param specimen_file=us__02__097__specimen__20130423fix999.txt \
  -param sample_file=us__02__097__sample__20130423fix999.txt \
  -param ssm_m_file=ssm__m__genome.wustl.edu_BRCA.IlluminaGA_DNASeq.Level_2.5.3.0.txt \
  -param ssm_p_file=ssm__p__genome.wustl.edu_BRCA.IlluminaGA_DNASeq.Level_2.5.3.0.txt \
  -param ssm_s_file=ssm__s__genome.wustl.edu_BRCA.IlluminaGA_DNASeq.Level_2.5.3.0.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=CLLE-ES \
  -param donor_file=es__10__005__donor__20130101.txt \
  -param specimen_file=es__10__005__specimen__20130101.txt \
  -param sample_file=es__10__005__sample__20130101.txt \
  -param ssm_m_file=ssm__es__10__005__m__5__20130101.txt \
  -param ssm_p_file=ssm__es__10__005__p__5__20130101.txt \
  -param ssm_s_file=ssm__es__10__005__s__5__20130101.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=CMDI-UK \
  -param donor_file=uk__24__093__donor__20120111.txt \
  -param specimen_file=uk__24__093__specimen__20120111.txt \
  -param sample_file=uk__24__093__sample__20120111.txt \
  -param ssm_m_file=ssm__uk__24__093__m__5__20120111.txt \
  -param ssm_p_file=ssm__uk__24__093__p__5__20120111.txt \
  -param ssm_s_file=ssm__uk__24__093__s__5__20120111.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=COAD-US \
  -param donor_file=us__22__097__donor__20130423fixed.txt \
  -param specimen_file=us__22__097__specimen__20130423fixed.txt \
  -param sample_file=us__22__097__sample__20130423fixed.txt \
  -param ssm_m_file=ssm__m__hgsc.bcm.edu_COAD.IlluminaGA_DNASeq.Level_2.1.5.0.txt____Merge.txt \
  -param ssm_p_file=ssm__p__hgsc.bcm.edu_COAD.IlluminaGA_DNASeq.Level_2.1.5.0.txt____Merge.txt \
  -param ssm_s_file=ssm__s__hgsc.bcm.edu_COAD.IlluminaGA_DNASeq.Level_2.1.5.0.txt____Merge.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=EOPC-DE \
  -param donor_file=de__28__030__donor__20121203.txt \
  -param specimen_file=de__28__030__specimen__20121203.txt \
  -param sample_file=de__28__030__sample__20121203.txt \
  -param ssm_m_file=ssm__de__28__030__m__60__20130808.txt \
  -param ssm_p_file=ssm__de__28__030__p__60__20130808.txt \
  -param ssm_s_file=ssm__de__28__030__s__60__20130808.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=ESAD-UK \
  -param donor_file=donor.txt \
  -param specimen_file=specimen.txt \
  -param sample_file=sample.txt \
  -param ssm_m_file=ssm__m.txt \
  -param ssm_p_file=ssm__p.txt \
  -param ssm_s_file=ssm__s.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=GACA-CN \
  -param donor_file=cn__06__009__donor__20110919.txt \
  -param specimen_file=cn__06__009__specimen__20110919.txt \
  -param sample_file=cn__06__009__sample__20110919.txt \
  -param ssm_m_file=ssm__cn__06__009__m__60__20110919.txt \
  -param ssm_p_file=ssm__cn__06__009__p__60__20110919.txt \
  -param ssm_s_file=ssm__cn__06__009__s__60__20110919.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=GBM-US \
  -param donor_file=us__03__097__donor__20130423fixed.txt \
  -param specimen_file=us__03__097__specimen__20130423fixed.txt \
  -param sample_file=us__03__097__sample__20130423fixed.txt \
  -param ssm_m_file=ssm__m__broad.mit.edu_GBM.IlluminaGA_DNASeq.Level_2.100.1.0.txt \
  -param ssm_p_file=ssm__p__broad.mit.edu_GBM.IlluminaGA_DNASeq.Level_2.100.1.0.txt \
  -param ssm_s_file=ssm__s__broad.mit.edu_GBM.IlluminaGA_DNASeq.Level_2.100.1.0.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=KIRC-US \
  -param donor_file=us__13__097__donor__20130423fixed.txt \
  -param specimen_file=us__13__097__specimen__20130423fixed.txt \
  -param sample_file=us__13__097__sample__20130423fixed.txt \
  -param ssm_m_file=ssm__m__hgsc.bcm.edu_KIRC.Mixed_DNASeq.Level_2.1.1.0.txt \
  -param ssm_p_file=ssm__p__hgsc.bcm.edu_KIRC.Mixed_DNASeq.Level_2.1.1.0.txt \
  -param ssm_s_file=ssm__s__hgsc.bcm.edu_KIRC.Mixed_DNASeq.Level_2.1.1.0.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=LAML-KR \
  -param donor_file=kr__15__POL__donor__20121115.txt \
  -param specimen_file=kr__15__POL__specimen__20121115.txt \
  -param sample_file=kr__15__POL__sample__20121115.txt \
  -param ssm_m_file=ssm__m.txt \
  -param ssm_p_file=ssm__p.txt \
  -param ssm_s_file=ssm__s.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=LAML-US \
  -param donor_file=us__15__095__donor__20130423fixed.txt \
  -param specimen_file=us__15__095__specimen__20130423fixed.txt \
  -param sample_file=us__15__095__sample__20130423fixed.txt \
  -param ssm_m_file=ssm__m__genome.wustl.edu_LAML.IlluminaGA_DNASeq.Level_2.2.14.0.txt \
  -param ssm_p_file=ssm__p__genome.wustl.edu_LAML.IlluminaGA_DNASeq.Level_2.2.14.0.txt \
  -param ssm_s_file=ssm__s__genome.wustl.edu_LAML.IlluminaGA_DNASeq.Level_2.2.14.0.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=LICA-FR \
  -param donor_file=fr__07__038__donor__20130618.txt \
  -param specimen_file=fr__07__038__specimen__20130618.txt \
  -param sample_file=fr__07__038__sample__20130618.txt \
  -param ssm_m_file=ssm__fr__07__038__m__60__20130621.txt \
  -param ssm_p_file=ssm__fr__07__038__p__60__20130621.txt \
  -param ssm_s_file=ssm__fr__07__038__s__60__20130621.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=LINC-JP \
  -param donor_file=jp__07__059__donor__20130815.txt \
  -param specimen_file=jp__07__059__specimen__20130815.txt \
  -param sample_file=jp__07__059__sample__20130815.txt \
  -param ssm_m_file=ssm__jp__07__059__m__60__20130815.txt \
  -param ssm_p_file=ssm__jp__07__059__p__60__20130815.txt \
  -param ssm_s_file=ssm__jp__07__059__s__60__20130815.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=LIRI-JP \
  -param donor_file=jp__07__0074__donor__20130813.txt \
  -param specimen_file=jp__07__0074__specimen__20130813.txt \
  -param sample_file=jp__07__0074__sample__20130813.txt \
  -param ssm_m_file=ssm__jp__07__0074__m__5__20130813.txt \
  -param ssm_p_file=ssm__jp__07__0074__p__5__20130813.txt \
  -param ssm_s_file=ssm__jp__07__0074__s__5__20130813.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=LUSC-US \
  -param donor_file=us__18__097__donor__20130423fixed.txt \
  -param specimen_file=us__18__097__specimen__20130423fixed.txt \
  -param sample_file=us__18__097__sample__20130423fixed.txt \
  -param ssm_m_file=ssm__m__broad.mit.edu_LUSC.IlluminaGA_DNASeq.Level_2.100.1.0.txt \
  -param ssm_p_file=ssm__p__broad.mit.edu_LUSC.IlluminaGA_DNASeq.Level_2.100.1.0.txt \
  -param ssm_s_file=ssm__s__broad.mit.edu_LUSC.IlluminaGA_DNASeq.Level_2.100.1.0.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=MALY-DE \
  -param donor_file=de__23__30__donor__20130815.txt \
  -param specimen_file=de__23__30__specimen__20130815.txt \
  -param sample_file=de__23__30__sample__20130815.txt \
  -param ssm_m_file=ssm__de__23__030__m__60__20130813.txt \
  -param ssm_p_file=ssm__de__23__030__p__60__20130813.txt \
  -param ssm_s_file=ssm__de__23__030__s__60__20130813.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=ORCA-IN \
  -param donor_file=in__09__062__donor__20130829.txt \
  -param specimen_file=in__09__062__specimen__20130829.txt \
  -param sample_file=in__09__062__sample__20130829.txt \
  -param ssm_m_file=ssm__in__09__062__m__60__20130830.txt \
  -param ssm_p_file=ssm__in__09__062__p__60__20130830.txt \
  -param ssm_s_file=ssm__in__09__062__s__60__20130830.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=OV-US \
  -param donor_file=us__05__097__donor__20130423fixed.txt \
  -param specimen_file=us__05__097__specimen__20130423fixed.txt \
  -param sample_file=us__05__097__sample__20130423fixed.txt \
  -param ssm_m_file=ssm__m__hgsc.bcm.edu_OV.SOLiD_DNASeq.Level_2.1.6.0.txt____Merge.txt \
  -param ssm_p_file=ssm__p__hgsc.bcm.edu_OV.SOLiD_DNASeq.Level_2.1.6.0.txt____Merge.txt \
  -param ssm_s_file=ssm__s__hgsc.bcm.edu_OV.SOLiD_DNASeq.Level_2.1.6.0.txt____Merge.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=PACA-AU \
  -param donor_file=donor.txt \
  -param specimen_file=specimen.txt \
  -param sample_file=sample.txt \
  -param ssm_m_file=ssm__m.txt \
  -param ssm_p_file=ssm__p.txt \
  -param ssm_s_file=ssm__s.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=PACA-CA \
  -param donor_file=donor.txt \
  -param specimen_file=specimen.txt \
  -param sample_file=sample.txt \
  -param ssm_m_file=ssm__m.txt \
  -param ssm_p_file=ssm__p.txt \
  -param ssm_s_file=ssm__s.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=PBCA-DE \
  -param donor_file=de__08__030__donor__20130815.txt \
  -param specimen_file=de__08__030__specimen__20130815.txt \
  -param sample_file=de__08__030__sample__20130815.txt \
  -param ssm_m_file=ssm__de__08__030__m__60__20130815.txt \
  -param ssm_p_file=ssm__de__08__030__p__60__20130815.txt \
  -param ssm_s_file=ssm__de__08__030__s__60__20130815.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=READ-US \
  -param donor_file=us__19__097__donor__20130423fixed.txt \
  -param specimen_file=us__19__097__specimen__20130423fixed.txt \
  -param sample_file=us__19__097__sample__20130423fixed.txt \
  -param ssm_m_file=ssm__m__hgsc.bcm.edu_READ.SOLiD_DNASeq.Level_2.1.6.0.txt____Merge.txt \
  -param ssm_p_file=ssm__p__hgsc.bcm.edu_READ.SOLiD_DNASeq.Level_2.1.6.0.txt____Merge.txt \
  -param ssm_s_file=ssm__s__hgsc.bcm.edu_READ.SOLiD_DNASeq.Level_2.1.6.0.txt____Merge.txt \
 ssm.pig

pig \
  -param run_name=load-prod-06e-40-23 \
  -param run_number=${run_number?} \
  -param project_id=UCEC-US \
  -param donor_file=us__21__097__donor__20130423fixed.txt \
  -param specimen_file=us__21__097__specimen__20130423fixed.txt \
  -param sample_file=us__21__097__sample__20130423fixed.txt \
  -param ssm_m_file=ssm__m__genome.wustl.edu_UCEC.IlluminaGA_DNASeq.Level_2.1.7.txt____Merge.txt \
  -param ssm_p_file=ssm__p__genome.wustl.edu_UCEC.IlluminaGA_DNASeq.Level_2.1.7.txt____Merge.txt \
  -param ssm_s_file=ssm__s__genome.wustl.edu_UCEC.IlluminaGA_DNASeq.Level_2.1.7.txt____Merge.txt \
 ssm.pig

date "+%y%m%d%H%M%S"
end=$(date +%s) && echo "ellapsed time: $[end-start]"

