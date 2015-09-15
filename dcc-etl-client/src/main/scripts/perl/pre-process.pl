#!/usr/bin/perl -w

#################################################################################################
#
# This script makes the assumption that the submission fiiles to be processed have already passed
# submission validation in ICGC12, although the checks were somewhat loosened.
#
# The 'fixes' we are trying here are not at all meant to be biologically correct, instead, the 
# objective is to 'fix' the submission files so that they can be further processed in ETL
# pipeline. For example, we ensure certain important fields, such as, assembly_version, mutation,
# mutation_type etc, are not -999/-888/-777
#
#################################################################################################

use lib 'lib';

use strict;

use Data::Dumper;
use Getopt::Long;
use File::Path;
use IO::Uncompress::Gunzip;
use IO::File;
use Submission::Util;

my @project_key;
my $input;
my $annotation_dir;
my $output;
my $force_overwrite = 0;
my $ssm_fix = 0;
my $conseq_profile = 0;
my $keep_ssm_unrelated_sample = 0;


GetOptions(
      "project_key:s" => \@project_key,
      "input:s" => \$input,
      "annotation_dir:s" => \$annotation_dir,
      "output:s" => \$output,
      "ssm_fix" => \$ssm_fix,
      "force" => \$force_overwrite,
      "keep_ssm_unrelated_sample" => \$keep_ssm_unrelated_sample,
      "conseq" => \$conseq_profile
   ) || die <<USAGE;
Usage:

$0 [-p project_key] [-o otuput_dir] [-f]

 -p Accepts one or more project_keys separated by comma (name of the folder under the submission folder).
    Will process all projects if none is specified.

 -o Output directory for processed projects, defaults to current working directory: .

 -i Input directory from where submission data are available, each sub-directory cooresponds to one project

 -a Input directory from where ssm annotation is available

 -k Flag for keeping donor/specimen/sample that do not have ssm data, by default those will be removed

 -s Do NOT use this for production! Flag for fixing ssm_p file, defaults to 0, ie, not try to fix ssm_p data, instead bad rows will be ignored

 -f Force overwrite to existing folder without warning.

USAGE

@project_key = uniq(split(/,/, join(',', @project_key))); # split multiple projects into array elements


my $settings = {
   "input_dir" => ($input || "/nfs/backups/icgc-submitted/icgc/submission/ICGC12"),
   #"input_dir" => "/Users/junjun/dcc2/submissions/release13",
   "annotation_dir" => ($annotation_dir || $input || "/Users/junjun/dcc2/submissions/release13"), # note annotation dir must contain both ssm_p and ssm_s files
#   "annotation_dir" => "/Users/junjun/dcc2/submissions/release12_annot", # note annotation dir must contain both ssm_p and ssm_s files
   "use_reannotation" => 1,
   "output_dir" => ($output || "."),
   "project_key" => \@project_key,
   "regex_donor" => qr".*?donor.*[\.txt|\.txt\.gz]$", # not very accurate, but good enough to find the file
   "regex_specimen" => qr".*?specimen.*[\.txt|\.txt\.gz]$",
   "regex_sample" => qr".*?sample.*[\.txt|\.txt\.gz]$",
   "regex_ssm_m" => qr"^ssm.*?__m.*[\.txt|\.txt\.gz]$",
   "regex_ssm_p" => qr"^ssm.*?__p.*[\.txt|\.txt\.gz]$",
   "regex_ssm_s" => qr"^ssm.*?__s.*[\.txt|\.txt\.gz]$",
   "donor_fields_strip" => ["donor_notes", "donor_region_of_residence"],
   "specimen_fields_strip" => ["specimen_notes"],
   "sample_fields_strip" => ["analyzed_sample_notes"],
   "remove_unrelated_sample" => !$keep_ssm_unrelated_sample,
   "fix_ssm" => $ssm_fix,  # flag for whether to fix ssm input files when required field (eg, reference genome allele, control genotype, mutation) is not populated
   "force_overwrite" => $force_overwrite,
   "conseq_profile" => $conseq_profile,
   "max_field_len" => 512
};

#print Dumper($settings);

# some varialbles
my $donor_to_keep = {};
my $specimen_to_keep = {};
my $sample_to_keep = {};
my $ssm_m_to_keep = {};
my $ssm_s_to_keep = {};

# verify settings etc before proceed

&verify_and_prepare($settings);

#print Dumper($settings);

die "\nNo project to process, done!\n" unless (@{$settings->{project_key}} > 0);

# now process one project at a time
foreach ( @{$settings->{project_key}} ) {

   # reset variables for a new project
   $donor_to_keep = {};
   $specimen_to_keep = {};
   $sample_to_keep = {};
   $ssm_m_to_keep = {};
   $ssm_s_to_keep = {};
   $settings->{sample_to_donor} = {};

   my $project_key = $_;
   $settings->{log_fh} = IO::File->new($settings->{output_dir} . "/" . $project_key . ".log", "w");

   my $success = 1;
   print "Start processing project: $project_key\n";

   ## create output dir for the current project, skip it if not created
   $success = 0 unless ($success && create_output_dir($project_key, $settings));

   ## we need to prepare a hash to keep mapping from sample to donor in order to
   ## be able to detect whether samples from the same donor have the same ssm reported multiple times
   ## we will need to scan the original three clinical files to get those three hashes populated, this is not
   ## so good as those files will be scanned twice, but there is not a way around it. It shouldn't be too bad
   ## though as these files are relatively small
   $success = 0 unless ($success && get_sample_map($project_key, $settings));

   ## process ssm_p file
   $success = 0 unless ($success && process_ssm_p($project_key, $settings, $ssm_m_to_keep, $ssm_s_to_keep));

   ## process ssm_m file
   $success = 0 unless ($success && process_ssm_m($project_key, $settings, $sample_to_keep, $ssm_m_to_keep));

   ## process ssm_s file
   $success = 0 unless ($success && process_ssm_s($project_key, $settings, $ssm_s_to_keep));

   ## process sample file
   $success = 0 unless ($success && process_sample($project_key, $settings, $specimen_to_keep, $sample_to_keep));

   ## process specimen file
   $success = 0 unless ($success && process_specimen($project_key, $settings, $donor_to_keep, $specimen_to_keep));

   ## process donor file
   $success = 0 unless ($success && process_donor($project_key, $settings, $donor_to_keep));

   if ($success) {
      print "Processing project: $project_key successfully completed.\n\n";
      $settings->{log_fh}->print ("Processing project: $project_key successfully completed.\n");
   } else {
      print "Processing project: $project_key terminated, see stderr for details.\n\n";
      $settings->{log_fh}->print ("Processing project: $project_key terminated before completion.\n");
   }

   $settings->{log_fh}->close;
}



##############
#
# subroutines
#
##############
sub field_too_long {
   my $fields = shift;

   foreach (@{$fields}) {
      return 1 if (length > $settings->{max_field_len});
   }

   return 0;
}

sub get_sample_map {
   my ($project_key, $settings) = @_;

   my $specimen_to_donor = {};

   # it's important not to change this variable value
   my $files = ["regex_specimen", "regex_sample"];

   for (my $f = 0; $f < @{$files}; $f++) {
      my ($input_fh, $output_fh) = open_files($files->[$f], $project_key, $settings);
   
      return 0 unless (defined $input_fh && defined $output_fh);
   
      my $i = 0;
      my $c_i = {};
      while(<$input_fh>){
         s/\r//g; # get rid of the insane ^M
   
         my @F = split /\t/;
         chomp $F[-1]; # trim new line

         if ($i == 0) { # header line
            print $output_fh $_;
            $c_i = column_name_to_index($_);
         }else{ # data lines
   
            if ($f == 0) {
               $specimen_to_donor->{$F[ $c_i->{specimen_id} ]} = $F[ $c_i->{donor_id} ];
            } else {
               $settings->{sample_to_donor}->{ $F[ $c_i->{analyzed_sample_id} ] } = $specimen_to_donor->{ $F[ $c_i->{specimen_id} ] } if (defined( $specimen_to_donor->{ $F[ $c_i->{specimen_id} ] } ));
            }
   
         }
         $i++;
      }
   
      $input_fh->close;
      $output_fh->close;
   }

   if ( keys %{$settings->{sample_to_donor}} == 0) {
      $settings->{log_fh}->print ("Looks like something is wrong in the clinical input files, no analyzed_sample_id can be mapped back to donors.\n");
      warn "Looks like something is wrong in the clinical input files, no analyzed_sample_id can be mapped back to donors.\n";
      return 0;
   } else {
      return 1;
   }
}


sub process_ssm_m {
   my ($project_key, $settings, $sample_to_keep, $ssm_m_to_keep) = @_;

   my ($input_fh, $output_fh) = open_files("regex_ssm_m", $project_key, $settings);

   return 0 unless (defined $input_fh && defined $output_fh);

   my $i = 0;
   my $c_i = {};
   my %analysis_sample_seen = ();
   while(<$input_fh>){
      s/\r//g; # get rid of the insane ^M

      my @F = split /\t/;
      chomp $F[-1];

      if (field_too_long(\@F)) {
         $settings->{log_fh}->print ("[WARN] 'ssm_m' row with field value too long: " . join ("\t", @F) . "\n");
      }
   

      if ($i == 0){

         $c_i = column_name_to_index($_) if ($i == 0);

         print $output_fh join ("\t", @F), "\n";

      }else{

         if (!$ssm_m_to_keep->{ $F[ $c_i->{analysis_id} ] }->{ $F[ $c_i->{analyzed_sample_id} ] }) {
            $i++;
            next;
         }

         # Let's make sure not to repeat the combination of analysis_id and analyzed_sample_id
         if (!$analysis_sample_seen{$F[ $c_i->{analysis_id} ]}->{$F[ $c_i->{analyzed_sample_id} ]}) {
            $analysis_sample_seen{$F[ $c_i->{analysis_id} ]}->{$F[ $c_i->{analyzed_sample_id} ]}++;
         } else {
            $i++;
            next;
         }

         $sample_to_keep->{ $F[ $c_i->{analyzed_sample_id} ] }++ if ($i > 0);
         $sample_to_keep->{ $F[ $c_i->{matched_sample_id} ] }++ if ($i > 0);

         # fix 'assembly_version' field if needed, UPDATED: we will not fix assembly_version any more, it's supposed to be fixed in Annotator
         #$F[ $c_i->{assembly_version} ] = &fix_assembly($project_key) if ($F[ $c_i->{assembly_version} ]  =~ /^-999|-888|-777$/);

         print $output_fh join ("\t", @F), "\n";
      }
      $i++;
   }

   $input_fh->close;
   $output_fh->close;
}


sub process_ssm_p {
   my ($project_key, $settings, $ssm_m_to_keep, $ssm_s_to_keep) = @_;

   my ($input_fh, $output_fh) = open_files("regex_ssm_p", $project_key, $settings);

   return 0 unless (defined $input_fh && defined $output_fh);

   my $i = 0;
   my $c_i = {};
   my %sample_ssm_p_seen = ();
   while(<$input_fh>){
      s/\r//g; # get rid of the insane ^M

      my @F = split /\t/;
      chomp $F[-1]; # trim new line

      if (field_too_long(\@F)) {
         $settings->{log_fh}->print ("[WARN] 'ssm_p' row with field value too long: " . join ("\t", @F) . "\n");
      }

      if ($i == 0) { # header line
         print $output_fh $_;
         $c_i = column_name_to_index($_);
      }else{ # data lines

         if ($settings->{fix_ssm}) {
            # fix 'reference_genome_allele' field if needed
            $F[ $c_i->{reference_genome_allele} ] = &rand_base()
                if (!$F[ $c_i->{reference_genome_allele} ] || $F[ $c_i->{reference_genome_allele} ] =~ /^-9999|-999|-888|-777$/);
   
           $F[ $c_i->{control_genotype} ] = $F[ $c_i->{reference_genome_allele} ] . "/" . $F[ $c_i->{reference_genome_allele} ]
                if (!$F[ $c_i->{control_genotype} ] || $F[ $c_i->{control_genotype} ] =~ /^-9999|-999|-888|-777$/);
   
           $F[ $c_i->{tumour_genotype} ] = $F[ $c_i->{reference_genome_allele} ] . "/" . &diff_base($F[ $c_i->{reference_genome_allele} ])
                if (!$F[ $c_i->{tumour_genotype} ] || $F[ $c_i->{tumour_genotype} ] =~ /^-9999|-999|-888|-777$/);
   
            # fix 'mutation' field if needed
            $F[ $c_i->{mutation} ] = &fix_mutation($F[ $c_i->{reference_genome_allele} ], $F[ $c_i->{control_genotype} ], $F[ $c_i->{tumour_genotype} ]) if ($F[ $c_i->{mutation} ] =~ /^-999|-888|-777$/);
   
            $F[ $c_i->{mutation} ] =~ s/\s+//g;  # remove whitespaces

            #mutation_type   chromosome      chromosome_start        chromosome_end  chromosome_strand   
            $F[ $c_i->{mutation_type} ] = "1" if ($F[ $c_i->{mutation_type} ] =~ /^-9999|-999|-888|-777$/);
            $F[ $c_i->{chromosome_strand} ] = "1" if ($F[ $c_i->{chromosome_strand} ] ne "1"); # ensure it's always positive strand
            $F[ $c_i->{chromosome} ] = int(rand(22)) + 1 if ($F[ $c_i->{chromosome} ] =~ /^-9999|-999|-888|-777$/);
            if ($F[ $c_i->{chromosome_start} ] =~ /^-9999|-999|-888|-777$/ || $F[ $c_i->{chromosome_start} ] =~ /^-9999|-999|-888|-777$/) {
               $F[ $c_i->{chromosome_start} ] = int(rand(51000000)) + 1;
               $F[ $c_i->{chromosome_end} ] = $F[ $c_i->{chromosome_start} ];
            }
   

         } else { # we don't try to fix ssm_p, now we just check whether important fields are all populated
            if (
                  $F[ $c_i->{reference_genome_allele} ] =~ /^-9999|-999|-888|-777$/ || $F[ $c_i->{reference_genome_allele} ] eq ""
                  || $F[ $c_i->{control_genotype} ] =~ /^-9999|-999|-888|-777$/ || $F[ $c_i->{control_genotype} ] eq ""
                  || $F[ $c_i->{tumour_genotype} ] =~ /^-9999|-999|-888|-777$/ || $F[ $c_i->{tumour_genotype} ] eq ""
                  || $F[ $c_i->{mutation} ] =~ /^-9999|-999|-888|-777$/ || $F[ $c_i->{mutation} ] eq ""
                  || $F[ $c_i->{mutation_type} ] =~ /^-9999|-999|-888|-777$/ || $F[ $c_i->{mutation_type} ] eq ""
                  || $F[ $c_i->{chromosome_strand} ] =~ /^-9999|-999|-888|-777$/ || $F[ $c_i->{chromosome_strand} ] eq ""
                  || $F[ $c_i->{chromosome} ] =~ /^-9999|-999|-888|-777$/ || $F[ $c_i->{chromosome} ] eq ""
                  || $F[ $c_i->{chromosome_start} ] =~ /^-9999|-999|-888|-777$/ || $F[ $c_i->{chromosome_start} ] eq ""
                  || $F[ $c_i->{chromosome_end} ] =~ /^-9999|-999|-888|-777$/ || $F[ $c_i->{chromosome_end} ] eq ""
                ) { # any of them is not populated

               $settings->{log_fh}->print ("important field missing, skip line in ssm_p file: " . join ("\t", @F), "\n");

               $i++;
               next;

            }
         }

         # Let us make sure not to repeat the same mutation for the same sample
         # this has been updated to not allow repeating same mutation for the same *DONOR*
         my $donor = $settings->{sample_to_donor}->{$F[ $c_i->{analyzed_sample_id} ]};

         unless (defined $donor) {
            $settings->{log_fh}->print ("The analyzed_sample_id: " . $F[ $c_i->{analyzed_sample_id} ] . " does not have cooresponding donor in donor file, skip line in ssm_p file: " . join ("\t", @F), "\n");

            $i++;
            next;
         }

         if (!$sample_ssm_p_seen{$donor}->{$F[ $c_i->{chromosome} ]}->{$F[ $c_i->{chromosome_start} ]}->{$F[ $c_i->{chromosome_end} ]}->{$F[ $c_i->{chromosome_strand} ]}->{$F[ $c_i->{mutation} ]}->{$F[ $c_i->{mutation_type} ]}) {
            $sample_ssm_p_seen{$donor}->{$F[ $c_i->{chromosome} ]}->{$F[ $c_i->{chromosome_start} ]}->{$F[ $c_i->{chromosome_end} ]}->{$F[ $c_i->{chromosome_strand} ]}->{$F[ $c_i->{mutation} ]}->{$F[ $c_i->{mutation_type} ]}++;
         } else {
            $i++;
            next;
         }

         # we enforce the cross reference integrity no matter fix_ssm option is set or not
         $ssm_m_to_keep->{$F[ $c_i->{analysis_id} ]}->{$F[ $c_i->{analyzed_sample_id} ]}++;
         $ssm_s_to_keep->{$F[ $c_i->{analysis_id} ]}->{$F[ $c_i->{analyzed_sample_id} ]}->{$F[ $c_i->{mutation_id} ]}++;


         print $output_fh join ("\t", @F), "\n";

      }
      $i++;
   }

   if (keys(%{$ssm_m_to_keep}) == 0) { # return 0 (ie, failed) if we did not try fix ssm and there is no ssm_p entry is valid at all
      warn "No rows in ssm_p is valid, skipping project: $project_key\n";
      return 0;
   }

   $input_fh->close;
   $output_fh->close;
}

sub process_ssm_s {
   my ($project_key, $settings, $ssm_s_to_keep) = @_;

   my ($input_fh, $output_fh) = open_files("regex_ssm_s", $project_key, $settings);

   return 0 unless (defined $input_fh && defined $output_fh);

   my $conseq_count = {};
   my $conseq_count_type = {};
   my $seen = {};
   my $current_entry = {};
   my $entry = [];

   my $i = 0;
   my $c_i = {};
   while(<$input_fh>){
      s/\r//g; # get rid of the insane ^M

      my @F = split /\t/;
      chomp $F[-1]; # trim new line

      if (field_too_long(\@F)) {
         $settings->{log_fh}->print ("[WARN] 'ssm_s' row with field value too long: " . join ("\t", @F) . "\n");
      }


      if ($i == 0) { # header line
         print $output_fh $_;
         $c_i = column_name_to_index($_);
      }else{ # data lines

         $F[ $c_i->{gene_affected} ] = "-999" if ($F[ $c_i->{transcript_affected} ] =~ /^-999|-888|-777$/);

         # skip this row if no consequence_type is reported
         if ($F[ $c_i->{consequence_type} ] =~ /^-999|-888|-777$/ || $F[ $c_i->{consequence_type} ] eq "") {
            $i++;
            next;
         }

         # skip this row if there is no cooresponding ssm_p entry
         if (!$ssm_s_to_keep->{ $F[ $c_i->{analysis_id} ] }->{ $F[ $c_i->{analyzed_sample_id} ] }->{ $F[ $c_i->{mutation_id} ] }) {
            $i++;
            next;
         }

         push @{$conseq_count->{$F[ $c_i->{mutation_id} ]. "\t" . $F[ $c_i->{transcript_affected} ]}}, $F[ $c_i->{consequence_type} ] if ($settings->{conseq_profile} && !($seen->{$F[ $c_i->{mutation_id} ]. "\t" . $F[ $c_i->{transcript_affected} ] . "\t" . $F[ $c_i->{consequence_type} ]}++)); 
         push @{$conseq_count_type->{$F[ $c_i->{mutation_id} ]. "\t" . $F[ $c_i->{consequence_type} ]}}, $F[ $c_i->{gene_affected} ] if ($settings->{conseq_profile} && !($seen->{$F[ $c_i->{mutation_id} ]. "\t" . $F[ $c_i->{consequence_type} ] . "\t" . $F[ $c_i->{gene_affected} ]}++)); 

         # below we buffer all consequences for the one mutation and ensure only one consequence is reported per conseqeuence type per transcript
         #print $output_fh join ("\t", @F), "\n"; # soon to be removed
         unless ( defined $current_entry->{ $F[ $c_i->{analysis_id} ] }->{ $F[ $c_i->{analyzed_sample_id} ] }->{ $F[ $c_i->{mutation_id} ] } ) {
            # different mutation entry is in, we need to process the current entry if ayn, then start to buffer the new entry
            # process the current entry if it's not empty
            &process_transcript_consequence($entry, $c_i, $output_fh);

            # reset the current entry
            $current_entry = {};
            $entry = [];
            
         }
         $current_entry->{ $F[ $c_i->{analysis_id} ] }->{ $F[ $c_i->{analyzed_sample_id} ] }->{ $F[ $c_i->{mutation_id} ] }++;
         push @{$entry}, \@F;

      }
      $i++;
   }

   # we need to process the last entry at the end of the loop
   &process_transcript_consequence($entry, $c_i, $output_fh);

   if ($settings->{conseq_profile}) {
      my $fh = IO::File->new($settings->{output_dir} . "/" . $project_key . ".conseq.by.trans", "w");
      foreach(keys(%{$conseq_count})){
         $fh->print ("$_\t" . (@{$conseq_count->{$_}} + 0) . "\t" . join(",", @{$conseq_count->{$_}}) . "\n");
      }
      $fh->close();

      $fh = IO::File->new($settings->{output_dir} . "/" . $project_key . ".conseq.by.type", "w");
      foreach(keys(%{$conseq_count_type})){
         $fh->print ("$_\t" . (@{$conseq_count_type->{$_}} + 0) . "\t" . join(",", @{$conseq_count_type->{$_}}) . "\n");
      }
      $fh->close();
   }

   $input_fh->close;
   $output_fh->close;
}

sub process_transcript_consequence {
   my ($entry, $c_i, $output_fh) = @_;

   my $transcript_conseq = {};
   foreach (@{$entry}) { # one row at a time
      my @F = @{$_}; # restore the original columns for each row
      if ($F[ $c_i->{transcript_affected} ] =~ /^ENST/) { # consequence that is related to a transcript
         push @{$transcript_conseq->{ $F[ $c_i->{transcript_affected} ] }}, \@F;
      } else {  # consequence that is not related to a transcript, report it as is
         print $output_fh join ("\t", @F), "\n";
      }
   }

   foreach (keys %$transcript_conseq) {
      my @conseq_lines = @{$transcript_conseq->{$_}};

      my @conseq;
      foreach (@conseq_lines) {
         push @conseq, $_->[ $c_i->{consequence_type} ];
      } 

      my $cons_position = &get_most_severe_conseq(\@conseq);

      if (defined $cons_position) {
         print $output_fh join ("\t", @{$conseq_lines[ $cons_position ]}), "\n";
      } else {
         $settings->{log_fh}->print ("[ERROR] unrecognizeable mutation consequence type: " . join ("\t", @{$conseq_lines[0]}), "\n");
      }
   }
}

sub get_most_severe_conseq {
   my $conseq = shift;

   my $conseq_order = [
      'frameshift_variant',
      'missense',
      'non_conservative_missense_variant',
      'initiator_codon_variant',
      'stop_gained',
      'stop_lost',
      'start_gained',
      'exon_lost',
      'coding_sequence_variant',
      'inframe_deletion',
      'inframe_insertion',
      'splice_region_variant',
      'regulatory_region_variant',
      'micro-rna',
      'non_coding_exon_variant',
      'nc_transcript_variant',
      '5_prime_UTR_variant',
      'five_prime_UTR',
      'upstream_gene_variant',
      'synonymous_variant',
      'stop_retained_variant',
      '3_prime_UTR_variant',
      'downstream_gene_variant',
      'intron_variant',
      'intergenic',
      'intergenic_variant',
      'intergenic_region'
   ];

   my %all_terms = (); # we just use the keys in the hash, it serves as a set
   foreach (@{$conseq_order}) {
      $all_terms{$_}++;
   }

   # we are taking a shortcut here when input contains only one consequence type, directly return 0
   return 0 if ( @{$conseq} == 1 && $all_terms{$conseq->[0]} );

   my $i = 0;
   my $conseq_to_pos = {};
   foreach (@{$conseq}) { # we assume items in the list of consequence is unique
      $conseq_to_pos->{$_} = $i;

      $i++;
   }

   foreach (@{$conseq_order}) { # find the first one and return
      return $conseq_to_pos->{$_} if (defined $conseq_to_pos->{$_});
   }

   return undef;
}

sub process_sample {
   my ($project_key, $settings, $specimen_to_keep, $sample_to_keep) = @_;

   my ($input_fh, $output_fh) = open_files("regex_sample", $project_key, $settings);

   return 0 unless (defined $input_fh && defined $output_fh);

   my $i = 0;
   my $c_i = {};
   while(<$input_fh>){
      s/\r//g; # get rid of the insane ^M

      my @F = split /\t/;
      chomp $F[-1]; # trim new line

      if (field_too_long(\@F)) {
         $settings->{log_fh}->print ("[WARN] 'sample' row with field value too long: " . join ("\t", @F) . "\n");
      }


      if ($i == 0) { # header line
         print $output_fh $_;
         $c_i = column_name_to_index($_);
      }else{ # data lines

         if ($settings->{remove_unrelated_sample} && !$sample_to_keep->{ $F[ $c_i->{analyzed_sample_id} ] }) {
            $i++;
            next;
         }

         $specimen_to_keep->{ $F[ $c_i->{specimen_id} ] }++; # add to specimen to keep

         # strip out possibly sensitive fields
         foreach (@{$settings->{sample_fields_strip}}) {
            $F[ $c_i->{$_} ]= "-777"; # assign -777 to controlled field, this will be replaced by "" in loader
         }

         print $output_fh join ("\t", @F), "\n";
      }
      $i++;
   }

   $input_fh->close;
   $output_fh->close;
}

sub process_specimen {
   my ($project_key, $settings, $donor_to_keep, $specimen_to_keep) = @_;

   my ($input_fh, $output_fh) = open_files("regex_specimen", $project_key, $settings);

   return 0 unless (defined $input_fh && defined $output_fh);

   my $i = 0;
   my $c_i = {};
   while(<$input_fh>){
      s/\r//g; # get rid of the insane ^M

      my @F = split /\t/;
      chomp $F[-1]; # trim new line

      if (field_too_long(\@F)) {
         $settings->{log_fh}->print ("[WARN] 'specimen' row with field value too long: " . join ("\t", @F) . "\n");
      }

      if ($i == 0) { # header line
         print $output_fh $_;
         $c_i = column_name_to_index($_);
      }else{ # data lines

         if ($settings->{remove_unrelated_sample} && !$specimen_to_keep->{ $F[ $c_i->{specimen_id} ] }) {
            $i++;
            next;
         }

         $donor_to_keep->{ $F[ $c_i->{donor_id} ] }++; # add to donor to keep

         # strip out possibly sensitive fields
         foreach (@{$settings->{specimen_fields_strip}}) {
            $F[ $c_i->{$_} ]= "-777"; # assign -777 to controlled field, this will be replaced by "" in loader
         }

         print $output_fh join ("\t", @F), "\n";
      }
      $i++;
   }

   $input_fh->close;
   $output_fh->close;
}

sub process_donor {
   my ($project_key, $settings, $donor_to_keep) = @_;

   my ($input_fh, $output_fh) = open_files("regex_donor", $project_key, $settings);

   return 0 unless (defined $input_fh && defined $output_fh);

   my $i = 0;
   my $c_i = {};
   while(<$input_fh>){
      s/\r//g; # get rid of the insane ^M

      my @F = split /\t/;
      chomp $F[-1]; # trim new line

      if (field_too_long(\@F)) {
         $settings->{log_fh}->print ("[WARN] 'donor' row with field value too long: " . join ("\t", @F) . "\n");
      }

      if ($i == 0) { # header line
         print $output_fh $_;
         $c_i = column_name_to_index($_);
      }else{ # data lines

         if ($settings->{remove_unrelated_sample} && !$donor_to_keep->{ $F[ $c_i->{donor_id} ] }) {
            $i++;
            next;
         }

         # strip out possibly sensitive fields
         foreach (@{$settings->{donor_fields_strip}}) {
            $F[ $c_i->{$_} ]= "-777"; # assign -777 to controlled field, this will be replaced by "" in loader
         }

         print $output_fh join ("\t", @F), "\n";
      }
      $i++;
   }

   $input_fh->close;
   $output_fh->close;
}

sub fix_assembly {
   my $project_key = shift;

   my $project_assembly = {
      "READ-US" => "2",
      "LAML-US" => "2",
      "COAD-US" => "2",
      "BLCA-US" => "1",
      "MALY-DE" => "1",
      "BRCA-US" => "1",
      "CESC-US" => "1",
      "UCEC-US" => "1",
      "STAD-US" => "1",
      "HNSC-US" => "1",
      "KIRC-US" => "1",
      "PRAD-US" => "1",
      "SKCM-US" => "1",
      "LUSC-US" => "1",
      "KIRP-US" => "1",
      "LGG-US" => "1",
      "THCA-US" => "1",
      "PBCA-DE" => "1",
      "CLLE-ES" => "1",
      "GBM-US" => "2",
      "OV-US" => "1",
      "LUAD-US" => "1"
   };

   return $project_assembly->{$project_key} || "1";
}

sub fix_mutation {
   my ($ref_genome_al, $cont_genotype, $tumor_genotype) = @_;

   my @cont_allele = split '/', $cont_genotype;
   my @tumor_allele = split '/', $tumor_genotype;

   foreach (@cont_allele) {
      my $cont_al = $_;
      foreach (@tumor_allele) {
         return $cont_al . '>' . $_ if ($cont_al ne $_);
      }
   }

   foreach (@tumor_allele) {
      return $ref_genome_al . '>' . $_ if ($ref_genome_al ne $_);
   }

   return $ref_genome_al . '>' . &diff_base($ref_genome_al);
}

# this function opens the input file returns a file handle, automatically handles .gz files
sub open_files {
   my ($regex, $project_key, $settings) = @_;

   # let's find the input file first
   # we hard-code a rule here to handle ssm_s file diffrently
   # UPDATE: always find ssm_p file in the annotation_dir
   # UPDATE 2: when use_reannotation is set, find ssm_m/p/s files in the annotation_dir
   my $source_dir = ($regex =~ /^regex_ssm_.+/ && $settings->{use_reannotation}) ? 
                    $settings->{annotation_dir} . "/" . $project_key :
                    $settings->{input_dir} . "/" . $project_key;

   my $file = find_file($source_dir, $settings->{$regex});
   
   unless (@{$file} == 1) { # fail it if not exactly one file is found
      warn "Found the following files match $regex pattern in $source_dir:\n  "
             . ( @{$file} ? join (", ", @{$file}) : "NONE") . "\nNot exactly one file is found, can NOT proceed with this project: $project_key\n";
      return ();
   }

   my $input_file = $source_dir . "/" . $file->[0];
   my $output_file = $settings->{output_dir} . "/" . $project_key . "/" . $file->[0];
   $output_file =~ s/\.gz$//;

   my $i_fh = $input_file =~ /\.gz$/ ? IO::Uncompress::Gunzip->new($input_file) : IO::File->new($input_file, "r");
   my $o_fh = IO::File->new($output_file, "w");

   return ($i_fh, $o_fh);
}


