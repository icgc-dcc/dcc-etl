
run_number=${1?}
date "+%y%m%d%H%M%S"
start=$(date +%s)

start_project=$(date +%s)
echo Starting: BOCA-UK
pig \
  -param run_name=load-prod-06e-41-26 \
  -param run_number=${run_number?} \
  -param project_id=BOCA-UK \
  -param donor_file=uk__02__093__donor__20130904.txt \
  -param specimen_file=uk__02__093__specimen__20130904.txt \
  -param sample_file=uk__02__093__sample__20130904.txt \
  -param stsm_m_file=stsm__uk__02__093__m__60__20130702.txt \
  -param stsm_p_file=stsm__uk__02__093__p__60__20130702.txt \
  -param stsm_s_file=stsm__uk__02__093__s__60__20130702.txt \
 stsm.pig
end_project=$(date +%s) && echo "ellapsed time: $[end_project-start_project]"

start_project=$(date +%s)
echo Starting: BRCA-UK
pig \
  -param run_name=load-prod-06e-41-26 \
  -param run_number=${run_number?} \
  -param project_id=BRCA-UK \
  -param donor_file=uk__02__093__donor__20130906.txt \
  -param specimen_file=uk__02__093__specimen__20130906.txt \
  -param sample_file=uk__02__093__sample__20130906.txt \
  -param stsm_m_file=stsm__uk__02__093__m__5__20130906.txt \
  -param stsm_p_file=stsm__uk__02__093__p__5__20130906.txt \
  -param stsm_s_file=stsm__uk__02__093__s__5__20130906.txt \
 stsm.pig
end_project=$(date +%s) && echo "ellapsed time: $[end_project-start_project]"

start_project=$(date +%s)
echo Starting: EOPC-DE
pig \
  -param run_name=load-prod-06e-41-26 \
  -param run_number=${run_number?} \
  -param project_id=EOPC-DE \
  -param donor_file=de__28__030__donor__20121203.txt \
  -param specimen_file=de__28__030__specimen__20121203.txt \
  -param sample_file=de__28__030__sample__20121203.txt \
  -param stsm_m_file=stsm__de__28__026__m__60__20130807.txt \
  -param stsm_p_file=stsm__de__28__026__p__60__20130807.txt \
  -param stsm_s_file=stsm__header__only__s__file.txt \
 stsm.pig
end_project=$(date +%s) && echo "ellapsed time: $[end_project-start_project]"

start_project=$(date +%s)
echo Starting: MALY-DE
pig \
  -param run_name=load-prod-06e-41-26 \
  -param run_number=${run_number?} \
  -param project_id=MALY-DE \
  -param donor_file=de__23__30__donor__20130815.txt \
  -param specimen_file=de__23__30__specimen__20130815.txt \
  -param sample_file=de__23__30__sample__20130815.txt \
  -param stsm_m_file=stsm__de__23__026__m__60__20130813.txt \
  -param stsm_p_file=stsm__de__23__026__p__60__20130813.txt \
  -param stsm_s_file=stsm__header__only__s__file.txt \
 stsm.pig
end_project=$(date +%s) && echo "ellapsed time: $[end_project-start_project]"

start_project=$(date +%s)
echo Starting: PACA-AU
pig \
  -param run_name=load-prod-06e-41-26 \
  -param run_number=${run_number?} \
  -param project_id=PACA-AU \
  -param donor_file=donor.txt \
  -param specimen_file=specimen.txt \
  -param sample_file=sample.txt \
  -param stsm_m_file=stsm__m.txt \
  -param stsm_p_file=stsm__p.txt \
  -param stsm_s_file=stsm__header__only__s__file.txt \
 stsm.pig
end_project=$(date +%s) && echo "ellapsed time: $[end_project-start_project]"

start_project=$(date +%s)
echo Starting: PBCA-DE
pig \
  -param run_name=load-prod-06e-41-26 \
  -param run_number=${run_number?} \
  -param project_id=PBCA-DE \
  -param donor_file=de__08__030__donor__20130815.txt \
  -param specimen_file=de__08__030__specimen__20130815.txt \
  -param sample_file=de__08__030__sample__20130815.txt \
  -param stsm_m_file=stsm__de__08__030__m__60__20130815.txt \
  -param stsm_p_file=stsm__de__08__030__p__60__20130815.txt \
  -param stsm_s_file=stsm__header__only__s__file.txt \
 stsm.pig
end_project=$(date +%s) && echo "ellapsed time: $[end_project-start_project]"

start_project=$(date +%s)
echo Starting: PEME-CA
pig \
  -param run_name=load-prod-06e-41-26 \
  -param run_number=${run_number?} \
  -param project_id=PEME-CA \
  -param donor_file=ca__08__007__donor__20120724.txt \
  -param specimen_file=ca__08__007__specimen__20120724.txt \
  -param sample_file=ca__08__007__sample__20120724.txt \
  -param stsm_m_file=stsm__ca__08__007__m__60__20120724.txt \
  -param stsm_p_file=stsm__ca__08__007__p__60__20120724.txt \
  -param stsm_s_file=stsm__ca__08__007__s__60__20120724.txt \
 stsm.pig
end_project=$(date +%s) && echo "ellapsed time: $[end_project-start_project]"

start_project=$(date +%s)
echo Starting: PRAD-UK
pig \
  -param run_name=load-prod-06e-41-26 \
  -param run_number=${run_number?} \
  -param project_id=PRAD-UK \
  -param donor_file=uk__28__093__donor__20121023.txt \
  -param specimen_file=uk__28__093__specimen__20121023.txt \
  -param sample_file=uk__28__093__sample__20121023.txt \
  -param stsm_m_file=stsm__uk__28__093__m__60__20121024.txt \
  -param stsm_p_file=stsm__uk__28__093__p__60__20121024.txt \
  -param stsm_s_file=stsm__uk__28__093__s__60__20121024.txt \
 stsm.pig
end_project=$(date +%s) && echo "ellapsed time: $[end_project-start_project]"


date "+%y%m%d%H%M%S"
end=$(date +%s) && echo "ellapsed time: $[end-start]"
