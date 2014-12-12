#!/usr/bin/perl

#################################################################################################
#
# This script is to convert old ICGC submission files into new format that conforms to the new
# dictionary
#
#################################################################################################

use FindBin;
use lib "$FindBin::Bin/lib";

use strict;
use Getopt::Long;
use Data::Dumper;
use Submission::Util;
use File::Path;
use IO::Uncompress::AnyUncompress;
use IO::Compress::Gzip;
use IO::Compress::Bzip2;
use IO::File;
use JSON;


my @project_key;
my @skip_data_type;
my @data_type_to_proc;
my $input;
my $output;
my $extra;
my $mongodb_uri;
my $change_file;
my $dictionary_file; 
my $force_overwrite = 0;
my $filter_controlled = 0;
my $summarize = 0; # flag for running summarization mode

GetOptions(
      "project_key:s" => \@project_key,
      "skip_data_type:s" => \@skip_data_type,
      "data_type_to_proc:s" => \@data_type_to_proc,
      "input:s" => \$input,
      "output:s" => \$output,
      "extra:s" => \$extra,
      "dictionary:s" => \$dictionary_file,
      "mongodb_uri:s" => \$mongodb_uri,
      "change_file:s" => \$change_file,
      "filter_controlled" => \$filter_controlled,
      "summarize" => \$summarize,
      "force" => \$force_overwrite
   ) || die <<USAGE;
Usage:

$0 [the following options]

 -p Accepts one or more project_keys separated by comma (name of the folder under the submission folder).
    Will process all projects if none is specified.

 -o Output directory for processed projects, defaults to current working directory: .

 -e Folder for extra data

 -i Input directory containing submissions

 -s takes a list of data type(s) that will be skipped, separate each data type by comma

 -m mongodb uri, ensure to include db name and collection name, format:  mongodb://username:password\@host:port/database.collection

 --force Force overwrite to existing folder without warning

 --filter_controlled Remove the controlled access fields 

USAGE

die "Must specify dictionary JSON file using --dictionary option!\n" unless ($dictionary_file);
die "Specified dictionary JSON file does not exist!\n" unless (-e $dictionary_file);


@project_key = uniq(split(/,|\s/, join(',', @project_key))); # split multiple projects into array elements

# if run in summarization mode, do not skip any data types, make sure all available data types are processed
# overwrite command line options for skip_data_type and data_type_to_proc no matter specified or not
my $summary = {};
my $specimen2donor = {};
my $sample2donor = {};
if ($summarize) {
    @skip_data_type = ();
    @data_type_to_proc = (
        "clinical.donor",
        "clinical.specimen",
        "clinical.sample",
        "ssm.ssm_m",
        "cnsm.cnsm_m",
        "stsm.stsm_m",
        "exp.exp_m",
        "pexp.pexp_m",
        "mirna.mirna_m",
        "jcn.jcn_m",
        "meth.meth_m",
        "sgv.sgv_m"
    );
}

@skip_data_type = uniq(split(/,/, join(',', @skip_data_type)));
my %skip_data_type = map { $_ => 1 } @skip_data_type;

@data_type_to_proc = uniq(split(/,/, join(',', @data_type_to_proc)));
my $data_type_to_proc = {};

foreach (@data_type_to_proc) {
    my ($data_type, $file_type) = split /\./;
    $data_type_to_proc->{$data_type}->{$file_type} = 1;
}

# some varialbles
my $data_types = {
    clinical => ["donor", "specimen", "sample", "therapy", "biomarker", "surgery", "exposure", "family"],
    ssm => ["ssm_m", "ssm_p"],
    cnsm => ["cnsm_m", "cnsm_p", "cnsm_s"],
    stsm => ["stsm_m", "stsm_p", "stsm_s"],
    exp => ["exp_m", "exp_g"],
    pexp => ["pexp_m", "pexp_p"],
    mirna => ["mirna_m", "mirna_p", "mirna_s"],
    jcn => ["jcn_m", "jcn_p"],
    sgv => ["sgv_m", "sgv_p"],
    meth => ["meth_m", "meth_p"],
    meth_array => ["meth_array_m", "meth_array_p", "meth_array_probes"],
};

# be very careful when set this max_row limit!!! This is really just a workaround for quickly checking the huge meth_p file
# only specified number of rows will be processed
my $max_row = {
    #"meth_p" => 100,
    #"meth_s" => 100,
    #"meth_array_p" => 100,
    #"mirna_p" => 100,
    #"exp_g" => 100,
    #"ssm_p" => 100,
    #"ssm_s" => 100,
    #"cnsm_p" => 100,
    #"jcn_p" => 100,
};

my $settings = {
   "input_dir" => ($input || "/Users/junjun/dcc2/submissions/release14"),
   #"input_dir" => ($input || "/nfs/dcc_secure/dcc/data/dev/prod/submission/ICGC14/"),
   #"input_dir" => ($input || "/hdfs/dcc/icgc/submission/ICGC13/"),
   "extra_data_dir" => ($extra || "/Users/junjun/dcc2/submissions/release13_extra"),
   "dictionary_file" => $dictionary_file,
   "change_file" => ($change_file || '{"change_file_not_specified":"yes"}'),
   "output_dir" => ($output || "."),
   "project_key" => \@project_key,
   "data_types" => $data_types,
   "force_overwrite" => $force_overwrite,
   "mongodb_uri" => ($mongodb_uri),
   "skip_bad_row" => 0
};


# verify settings etc before proceed
&verify_and_prepare($settings);

# read in and parse dictionary to a simpler version with min. information available
$settings->{easy_dict} = easy_dict($settings->{dictionary_file});

$settings->{changes} = parse_json_file($settings->{change_file});

#print Dumper($settings);

die "\nNo project to process, done!\n" unless (@{$settings->{project_key}} > 0);

my $extra_data = {};
my $sequencing_strategy = {};

# now process one project at a time
foreach ( @{$settings->{project_key}} ) {
   # reset variables for a new project

   $extra_data = {};
   $sequencing_strategy = {};

   my $project_key = $_;
   $settings->{log_fh} = IO::File->new($settings->{output_dir} . "/reports/" . $project_key . ".report", "w");

   my $success = 1;
   print "Start processing project: $project_key\n";

   $settings->{log_fh}->print ("[INFO] Start processing project: $project_key, dictionary file: ".$settings->{dictionary_file}.", change file: ".$settings->{change_file}.".\n");

   ## create output dir for the current project, skip it if not created
   $success = 0 unless ($success && create_output_dir($project_key, $settings));

   ## read and process any extra_data files if any
   
   $extra_data = &load_extra_data($project_key) if (-d $settings->{extra_data_dir});

   ## now starts to process all available data types for a project
   # make sure data types are processed in fixed order and clinical data is processed first
   my @all_data_types;
   foreach (sort keys (%{$data_types}) ) {
      push @all_data_types, $_ unless ($_ eq 'clinical');
   }
   unshift @all_data_types, 'clinical';

   foreach (@all_data_types) {
      my $current_data_type = $_;

      # skip data type if it is specified by the user
      next if ($skip_data_type{$current_data_type});

      # skip the data type if user defined data types to be process, but the current one is not one of them
      next if (keys %{$data_type_to_proc} && !$data_type_to_proc->{$current_data_type});

      # process one file type at a time
      my $prev_file_type;
      foreach (@{$data_types->{$current_data_type}}) {
         my $file_type = $_;

         next if (keys %{$data_type_to_proc} && !($data_type_to_proc->{$current_data_type}->{''} || $data_type_to_proc->{$current_data_type}->{$file_type}));

         my $ret = process_file($project_key, $file_type, $prev_file_type);

         $success = 0 unless ($success && $ret);

         undef $settings->{log}; # reset log for each file type

         $prev_file_type = $file_type if ($ret == 1);  # remember previous file type that has been successfully done in real input file, this is for generation header-only file
      }

   }

   if ($settings->{mongodb_uri} && ($project_key eq "ALL-US" || $project_key eq "NBL-US" || !($project_key =~ /-US$/))) { # we limit this to non-TCGA projects, we can run this for all projects if needed
      $success = 0 unless ($success && generate_sequencing_strategy_data($project_key, $sequencing_strategy, $settings->{mongodb_uri}));
      unless ($success) {
         warn "[ERROR] Something wrong when load sequencing analysis data into mongodb. Check mongodb is accessible.\n";
         $settings->{log_fh}->print ("[ERROR] Something wrong when load sequencing analysis data into mongodb. Check mongodb is accessible.\n");
      }

   }

   # report summary if running in summarization mode
   if ($summarize) {
      my $summary_report_file = IO::File->new($settings->{output_dir} . "/reports/" . $project_key . ".summary.json", "w");
      for (sort keys %$summary) {
         $summary_report_file->print( to_json($summary->{$_}) . "\n" );
      }
   }

   if ($success) {
      print "Processing project: $project_key successfully completed.\n\n";
      $settings->{log_fh}->print ("[INFO] Processing project: $project_key successfully completed.\n");
   } else {
      print "Processing project: $project_key terminated, see stderr for details.\n\n";
      $settings->{log_fh}->print ("[ERROR] Processing project: $project_key terminated before completion.\n");
   }

   $settings->{log_fh}->close;

}


sub load_extra_data {
   my ($project_key) = @_;

   my $extra_data = {};

   my $extra_data_dir = $settings->{extra_data_dir};

   my $files = find_file($extra_data_dir, "^$project_key\\..+");

   foreach (@{$files}){
      if (/^$project_key\.(.+?)\.(.+?)\.txt$/) {
          my ($file_type, $col) = ($1, $2);

          open (IN, "< $extra_data_dir/$_");
          while (<IN>) {  # the file contains key-value pair per line
             s/\n//;
             s/\r//g;

             my @F = split /\t/;

             $extra_data->{$file_type}->{$col}->{$F[0]} = $F[1];
          }
          close (IN);
      }
   }
   
   return $extra_data;
}


sub process_file {
   my ($project_key, $file_type, $prev_file_type) = @_;

   my $ret = -1;

   my $files = find_file($settings->{input_dir}."/".$project_key, defined $settings->{easy_dict}->{$file_type} ? $settings->{easy_dict}->{$file_type}->{pattern} : "4e119331-146f-46bd-9dd4-4e955edba8b9-202a935e-28c1-40c2-b357-f5866444127a"); # quick workaround to avoid exception when dictionary does not contain certain data type, eg, meth

   #print $settings->{input_dir}."/".$project_key."---".$file_type."---".$settings->{easy_dict}->{$file_type}->{pattern} . "\n";

   if ($#{$files} == -1 && $file_type =~ /.+_s$/ && $prev_file_type =~ /.+_p$/) { ## no file found for the current file type, if the type is *_s then generate a file with only header line, this file is required for Loader
      my $file_name = $file_type;

      $file_name =~ s/_s/__header__only__s__file.txt/;

      my $o_fh = IO::File->new($settings->{output_dir} . "/" . $project_key . "/" . $file_name, "w");
      print $o_fh join ("\t", @{$settings->{easy_dict}->{$file_type}->{field_list}})."\n";

      $o_fh->close();

      $ret = 1;

   }else{  # now process the files

      my $i = 0;
      my $i_fh; # input file handler
      my $o_fh; # output file handler

      foreach (@{$files}){  # in old submissions, one file type may consists of multiple files, we process them one by one and output to one single file as required in the new submission system
         ($i_fh, $o_fh) = create_file_with_header_return_fh($_, $project_key, $file_type, $settings) if ($i == 0);
   
         file_conversion($i_fh, $o_fh, $file_type, $settings, $project_key);
   
         $i++;
      }

      $i_fh->close() if defined $i_fh;
      $o_fh->close() if defined $o_fh;

      $ret = 1 if ($i);

   }

   return $ret;
}


sub create_file_with_header_return_fh {
   my ($file, $project_key, $file_type, $settings) = @_;

   print "processing $file of project: $project_key\n";

   my $input_file = $settings->{input_dir} . "/" . $project_key . "/" . $file;

   my $i_fh = $input_file =~ /\.gz$|\.bz2$/ ? IO::Uncompress::AnyUncompress->new($input_file) : IO::File->new($input_file, "r");

   # now we decided to go with uncompressed output
   #my $o_fh = $output_file =~ /\.gz$/ ? IO::Compress::Gzip->new($output_file) :
   #        ( $output_file =~ /\.bz2$/ ? IO::Compress::Bzip2->new($output_file) : IO::File->new($output_file, "w"));

   my $output_file = $settings->{output_dir} . "/" . $project_key . "/" . $file;

   $output_file =~ s/\.txt\.gz$|\.txt\.bz2$/.txt/;

   my $o_fh = IO::File->new($output_file, "w");

   my $header = join ("\t", @{$settings->{easy_dict}->{$file_type}->{field_list}});

   print $o_fh "$header\n";

   return ($i_fh, $o_fh);
}


sub file_conversion {
   my ($i_fh, $o_fh, $file_type, $settings, $project_key) = @_;

   my @new_cols = @{$settings->{easy_dict}->{$file_type}->{field_list}};

   my $i = 0;
   my $c_i = {};

OUTER:
   while(<$i_fh>){
      s/\r//g; # get rid of the insane ^M

      my @F = split /\t/;
      chomp $F[-1]; # trim new line

      if ($i == 0) { # header line

         $c_i = column_name_to_index($_);

         # check for unrecognizable columns in submission file
         my @extra_fields = &check_extra_fields($file_type, $c_i, $settings->{easy_dict}->{$file_type}->{fields}, $settings->{changes});
         foreach (@extra_fields) {
            $settings->{log_fh}->print("[WARN] Extra field: '$_' found in file type: $file_type\n");
         }

      }else{ # data lines

         my $new_line = "";

         # use existing value for column with the same name exists and it's not defined as required (as required field will need to conform certain additional rules)

         my $failed = 0;
         my $new_c_i = {};
         my $i = 0;
         foreach (@new_cols) {
            $new_c_i->{$_} = $i;
            if ($project_key =~ /-US$/ && $file_type eq "specimen") { # hard-code this for now

               $F[ $c_i->{$_} ] = "" if ($_ eq "digital_image_of_stained_section"); # reset this to empty strings for all TCGA projects, as the original value is just bogus

               if ($extra_data->{$file_type}->{$_}->{ $F[ $c_i->{specimen_id} ] }) {  # find out whether we need to repopulate the field with data from extra data file, hard-code for specimen data type only for now
                  $F[ $c_i->{$_} ] = $extra_data->{$file_type}->{$_}->{ $F[ $c_i->{specimen_id} ] };
               }
            }

            #BDO, I added this to blank out any fields that are controlled access if the --filter_controlled option is given
            if ($filter_controlled && defined $settings->{easy_dict}{$file_type}{controlled_fields}{$_}) {
               #print "FIELD: $_ FILE TYPE: $file_type CONTROLLED?: $settings->{easy_dict}{$file_type}{controlled_fields}{$_}\n";
               $new_line .= "\t";
            }elsif (defined $c_i->{$_} && !( $settings->{easy_dict}->{$file_type}->{fields}->{$_} )) {
               $new_line .= $F[ $c_i->{$_} ] . "\t";
            }else{
               my $new_value = get_new_value($_, $file_type, $settings, \@F, $c_i);
               if (ref $new_value eq 'HASH') { # failed
                  $failed = 1;
               }else{
                  $new_line .= $new_value . "\t";
               }
            }

            $i++;
         }

         next if $failed;  # skip the row if any of the columns failed

         chop $new_line;

         # TODO: put this in a function, later
         # here we intercept the final data (ready for output) to get necessary information to generate sequencing stretagy data
         my @Cols = split /\t/, $new_line;

         if ($file_type eq "sample") { # get sample type for each sample in the current project
            $sequencing_strategy->{sample_type}->{$Cols[ $new_c_i->{analyzed_sample_id} ]} = &sample_type($Cols[ $new_c_i->{analyzed_sample_type} ]);
         }

         if ($file_type =~ /_m$/ && defined($new_c_i->{sequencing_strategy})) { # one of the meta files and has sequencing_strategy field

            my $strategy = &sequencing_strategy( $Cols[ $new_c_i->{sequencing_strategy} ] );
            my $raw_data_repository = &repository($Cols[ $new_c_i->{raw_data_repository} ]) || "";
            my $raw_data_accession = $Cols[ $new_c_i->{raw_data_accession} ];
            $raw_data_accession = "" if ($raw_data_accession =~ /^-/); # convert to empty string if accession starts with '-', -777, -888 etc

            if ($strategy) {
               $sequencing_strategy->{data}->{$Cols[ $new_c_i->{analyzed_sample_id} ]}->{$strategy} = [$raw_data_repository, $raw_data_accession];
            }

         }

         # collect information for summary when running in summarization model
         if ($summarize) {
            if ($file_type eq "donor") { # add a new donor to the summary
               $summary->{ $Cols[ $new_c_i->{donor_id} ] } = {
                   submitted_donor_id => $Cols[ $new_c_i->{donor_id} ],
                   project_code => $project_key
               }; 
            }

            # populate the specimen2donor map
            $specimen2donor->{ $Cols[ $new_c_i->{specimen_id} ] } = $Cols[ $new_c_i->{donor_id} ] if ($file_type eq "specimen"); 

            # populate the sample2donor map
            $sample2donor->{ $Cols[ $new_c_i->{analyzed_sample_id} ] } = $specimen2donor->{ $Cols[ $new_c_i->{specimen_id} ] } if ($file_type eq "sample"); 

            if ($file_type =~ /_m$/) { # meta data file
               my $type = $file_type;
               $type =~ s/_m$//;

               if (defined $summary->{ $sample2donor->{ $Cols[ $new_c_i->{analyzed_sample_id} ] } }) {
                  $summary->{ $sample2donor->{ $Cols[ $new_c_i->{analyzed_sample_id} ] } }->{"_".$type."_exists"} = "true" unless ($type eq "ssm");

                  # populated the _available_data_type list
                  if (defined $summary->{ $sample2donor->{ $Cols[ $new_c_i->{analyzed_sample_id} ] } }->{_available_data_type}) {
                     my $exists = 0;
                     foreach (@{ $summary->{ $sample2donor->{ $Cols[ $new_c_i->{analyzed_sample_id} ] } }->{_available_data_type} }) {
                        $exists = 1 if ($_ eq $type);
                     }
                     push @{ $summary->{ $sample2donor->{ $Cols[ $new_c_i->{analyzed_sample_id} ] } }->{_available_data_type} }, $type unless $exists;
                  } else {
                     $summary->{ $sample2donor->{ $Cols[ $new_c_i->{analyzed_sample_id} ] } }->{_available_data_type} = [$type];
                  }

               } else {
                  log_once ("[WARN] data type: $type, analyzed_sample_id: ". $Cols[ $new_c_i->{analyzed_sample_id} ] ." does not have any matched donor\n", $settings);
               }
            }

         }

         print $o_fh "$new_line\n" unless ($summarize); # don't output actually data file if running in summarization mode
         
      }
      $i++;

      last if (defined $max_row->{$file_type} && $i > $max_row->{$file_type});
   }

}

# function to detect and report extra fields that does not exist in new dictionary and is not defined in change file
sub check_extra_fields {
   my ($file_type, $c_i, $dict_fields, $changes) = @_;

   my @extra_fields = ();

   foreach (keys %{$c_i}) {
      next if defined $dict_fields->{$_};

      my $field = $_;

      if ($changes->{$file_type} eq undef) {
         push @extra_fields, $field;
      } else {
         my $defined = 0;
         foreach (keys %{$changes->{$file_type}}) {
            if ( $changes->{$file_type}->{$_}->{changed_from} eq $field ) {
               $defined = 1;
               last;
            }
         }
         push @extra_fields, $field unless $defined;
      }
   }

   return @extra_fields;
}

sub get_new_value {
   my ($col, $file_type, $settings, $input_cols, $c_i) = @_;

   my $changes = $settings->{changes}->{$file_type};

   my $required = $settings->{easy_dict}->{$file_type}->{fields}->{$col};

   my $new_value = '-999';

   # missing column not defined in changes file, bad, it's not too late, we define it now as itself
   $changes->{$col}->{changed_from} = $col unless (defined $changes->{$col}->{changed_from});

   if ($required == 0) {

      if (defined $c_i->{ $changes->{$col}->{changed_from} } && defined $input_cols->[$c_i->{ $changes->{$col}->{changed_from} }]) {
         $new_value = $input_cols->[$c_i->{ $changes->{$col}->{changed_from} }];
    
         log_once("[INFO] Column: '$col' in '$file_type' is defined as being changed from '" . $changes->{$col}->{changed_from} . "', value from old submission is used\n", $settings);
    
      }elsif(defined $changes->{$col}->{default_value}){
         $new_value = $changes->{$col}->{default_value};
    
         log_once("[INFO] Column: '$col' in '$file_type' uses the defined default value: '".$new_value."'\n", $settings);
      }else{
         log_once("[INFO] Column: '$col' in '$file_type' uses -999. Neither old value nor default value is available\n", $settings);
      }

   }elsif ($required == 1) {

      if (defined $c_i->{ $changes->{$col}->{changed_from} } && $input_cols->[$c_i->{ $changes->{$col}->{changed_from} }] ne "-999") {
         $new_value = $input_cols->[$c_i->{ $changes->{$col}->{changed_from} }];
    
         log_once("[INFO] Column: '$col' in '$file_type' is defined as being changed from '" . $changes->{$col}->{changed_from} . "', value from old submission is used\n", $settings);
    
      }elsif(defined $changes->{$col}->{default_value} && $changes->{$col}->{default_value} ne "-999"){
         $new_value = $changes->{$col}->{default_value};
    
         log_once("[WARN] Column: '$col' in '$file_type' uses defined default value: '".$new_value."'\n", $settings);
      }else{

         $new_value = '-888';
 
         log_once("[WARN] Column: '$col' in '$file_type' uses -888. Neither value from old submission nor any defined default value is suitable as this field is defined as required.\n", $settings);
      }
   }elsif ($required == 2) {
      if (defined $c_i->{ $changes->{$col}->{changed_from} } && !($input_cols->[$c_i->{ $changes->{$col}->{changed_from} }] =~ /^-999\z|^-888\z|^-777\z|^\z/)) {
         $new_value = $input_cols->[$c_i->{ $changes->{$col}->{changed_from} }];
    
         log_once("[INFO] Column: '$col' in '$file_type', as strictly required field, is defined as being changed from '" . $changes->{$col}->{changed_from} . "', value from old submission is used\n", $settings);
    
      }elsif(defined $changes->{$col}->{default_value} && !($changes->{$col}->{default_value} =~ /^-999\z|^-888\z|^-777\z/)){ # update: allow empty string for strictly required field
         $new_value = $changes->{$col}->{default_value};
    
         log_once("[WARN] Column: '$col' in '$file_type', as strictly required field, uses the defined default value: '".$new_value."'\n", $settings);
      }else{
 
         log_once("[ERROR] Column: '$col' in '$file_type', as strictly required field, no suitable value from old submission or defined default can be used!\n", $settings);

         $settings->{skip_bad_row} ? $new_value = {} : $new_value = "-9999";
      } 
   }

   return $new_value;
}


sub log_once {
   my ($msg, $settings) = @_;

   $settings->{log_fh}->print($msg) unless ($settings->{log}->{$msg}++);

}


sub sample_type {
   my $code = shift;

   my %code2type = (
      1 => 'Normal blood',
      2 => 'Leukemic blood',
      3 => 'Normal control adjacent to primary',
      4 => 'Normal control from non-tumour site',
      5 => 'Control from cell line derived from normal tissue',
      6 => 'Normal mouse host',
      7 => 'Primary tumour',
      8 => 'Mouse xenograft derived from tumour',
      9 => 'Cell line derived from tumour',
      10 => 'Cell line derived from xenograft',
      11 => 'Other'
   );

   return $code2type{$code};

}

sub sequencing_strategy {
   my $code = shift;

   my %code2type = (
      1 => 'WGS',
      2 => 'WGA',
      3 => 'WXS',
      4 => 'RNA-Seq',
      5 => 'miRNA-Seq',
      6 => 'ncRNA-Seq',
      7 => 'WCS',
      8 => 'CLONE',
      9 => 'POOLCLONE',
      10 => 'AMPLICON',
      11 => 'CLONEEND',
      12 => 'FINISHING',
      13 => 'ChIP-Seq',
      14 => 'MNase-Seq',
      15 => 'DNase-Hypersensitivity',
      16 => 'Bisulfite-Seq',
      17 => 'EST',
      18 => 'FL-cDNA',
      19 => 'CTS',
      20 => 'MRE-Seq',
      21 => 'MeDIP-Seq',
      22 => 'MBD-Seq',
      23 => 'Tn-Seq',
      24 => 'VALIDATION',
      25 => 'FAIRE-seq',
      26 => 'SELEX',
      27 => 'RIP-Seq',
      28 => 'ChIA-PET',
      29 => 'OTHER',
      30 => 'non-NGS'
   );

   if ($code2type{$code}) {
      return $code2type{$code};
   } else {  # this is to handle the situation that user submitted sequencing_strategy with value instead of code
      my %types = reverse %code2type;
      if ($types{$code}) {
         return  $code;
      }else{
         return undef;
      }
   }

}

sub repository {
   my $code = shift;

   my %code2type = (
      1 => 'EGA',
      2 => 'dbSNP',
      3 => 'TCGA',
      4 => 'CGHub',
      5 => 'GEO'
   );

   return $code2type{$code};
}
