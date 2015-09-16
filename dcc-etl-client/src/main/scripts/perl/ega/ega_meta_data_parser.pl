#!/usr/bin/perl

use strict;
use open ":std", ":encoding(UTF-8)";
use Getopt::Long;
use Data::Dumper;
use XML::DOM;
use XML::Parser;

my $input;
my $output;
my @study;

GetOptions(
      "input:s" => \$input,
      "output:s" => \$output,
      "studies:s" => \@study
   ) || die <<USAGE;
Usage:

$0 [the following options]

 -i Specify the directory containing studies' xml files, each study is a subdirectory

 -s Study (or studies). Use comma to separate multiple studies if more than one is specified

 -o Output directory for processed studies, defaults to current working directory

USAGE

@study = uniq(split(/,/, join(',', @study))); # split multiple projects into uniq array elements

my $settings = {
   "input_dir" => ($input || "ICGC_XMLs"),
   "output_dir" => ($output || "."),
   "study" => \@study
};


# verify settings etc before proceed
&verify_and_prepare($settings);

my $parser = new XML::DOM::Parser;

# process one study at a time

foreach (@{$settings->{study}}) {

  my $output = []; # initiate the report for each study, a 2D array

  my $study = $_;
  my $study_accession = $study;
  $study_accession =~ s/_.+$//;

  $settings->{log_fh} = IO::File->new($settings->{output_dir} . "/" . $study . ".log", "w");

  my $success = 1;
  print "Start processing study: $study\n";

  $settings->{log_fh}->print ("[INFO] Start processing study: $study.\n");

  # do some preparation here, build maps for exp_accession2file, sample_accession2file
  my $exp_accession2file = build_accession2file($settings->{input_dir}."/".$study, '^EGAX\d{11}_experiment\.xml$', 'EXPERIMENT', 'accession');
  my $sample_accession2file = build_accession2file($settings->{input_dir}."/".$study, '^EGAN\d{11}_sample\.xml$', 'SAMPLE', 'accession');

  # process policy files, one at a time. One study may have one or many policy/dataset files
  my $policy_files = find_file($settings->{input_dir}."/".$study, '^EGAP\d{11}_policy\.xml$');

  foreach (@{$policy_files}) {
    if (-z $settings->{input_dir}."/".$study."/".$_) {
      print "  Policy file empty, skipping: $_\n";
      next;
    }
    my $doc = $parser->parsefile($settings->{input_dir}."/".$study."/".$_, ProtocolEncoding => 'ISO-8859-1');

    my $dac = $doc->getElementsByTagName('DAC_REF')->item(0)->getAttributeNode('accession')->getNodeValue;
    next if ($dac ne "EGAC00001000010"); # skip it if it's not ICGC

    my $policy_accession = $doc->getElementsByTagName('POLICY')->item(0)->getAttributeNode('accession')->getNodeValue;

    my $dataset_file = find_file($settings->{input_dir}."/".$study, $policy_accession.'_dataset.xml$');

    if (@{$dataset_file} == 0) {
      print "  No dataset file associated with this policy file: $study"."/"."$_\n";
      $settings->{log_fh}->print ("[WARN] No dataset file associated with this policy file: $study"."/"."$_\n");
      next;
    }

    if (-z $settings->{input_dir}."/".$study."/".$dataset_file->[0]) {
      print "  Dataset file empty, skipping: ".$dataset_file->[0]."\n";
      $settings->{log_fh}->print ("[WARN] Dataset file empty, skipping: ".$dataset_file->[0]."\n");
      next;
    }
      
    # parse dataset file
    $doc = $parser->parsefile($settings->{input_dir}."/".$study."/".$dataset_file->[0], ProtocolEncoding => 'ISO-8859-1');
    my $dataset_accession = $doc->getElementsByTagName('DATASET')->item(0)->getAttributeNode('accession')->getNodeValue;
    my $center_name = $doc->getElementsByTagName('DATASET')->item(0)->getAttributeNode('center_name')->getNodeValue;
    my $title = $doc->getElementsByTagName('TITLE')->item(0)->getFirstChild->getNodeValue();


    # get all run references
    my $run_ref = $doc->getElementsByTagName('RUN_REF');

    for( my $i = 0; $i < $run_ref->getLength; $i++ ) {  # iterate all runs
      my $run_accession = $run_ref->item($i)->getAttributeNode('accession')->getNodeValue;

      my $run_file = find_file($settings->{input_dir}."/".$study, $run_accession.'_run.xml$');

      if (@{$run_file} == 0) {
        print "  Referred RUN file does not exist: $run_accession"."_run.xml\n";
        $settings->{log_fh}->print ("[WARN] Referred RUN file does not exist: $run_accession"."_run.xml\n");
        next;
      }

      if (-z $settings->{input_dir}."/".$study."/".$run_file->[0]) {
        print "  Run file empty, skipping: ".$run_file->[0]."\n";
        $settings->{log_fh}->print ("[WARN] Run file empty, skipping: ".$run_file->[0]."\n");
        next;
      }

      # now parse run file
      $doc = $parser->parsefile($settings->{input_dir}."/".$study."/".$run_file->[0], ProtocolEncoding => 'ISO-8859-1');

      $run_accession = $doc->getElementsByTagName('RUN')->item(0)->getAttributeNode('accession')->getNodeValue;

      my $exp_ref = $doc->getElementsByTagName('EXPERIMENT_REF')->item(0)->getAttributeNode('accession')->getNodeValue;

      my $exp_file = $exp_accession2file->{$exp_ref};
      unless (defined $exp_file) {
        print "  Skip exp accession: $exp_ref, no associated experiment file exists\n";
        $settings->{log_fh}->print ("[WARN] Skip exp accession: $exp_ref, no associated experiment file exist\n");
        next;
      }


      my $seq_files = $doc->getElementsByTagName('FILE');

      my $seq_file_type_name;
      for( my $k = 0; $k < $seq_files->getLength; $k++ ) {
        my $file_name = $seq_files->item($k)->getAttributeNode('filename')->getNodeValue;
        my $file_type = $seq_files->item($k)->getAttributeNode('filetype')->getNodeValue;

        $seq_file_type_name .= "$file_type:$file_name,";
      }
      chop $seq_file_type_name;

      # now parse experiment file
      $doc = $parser->parsefile($settings->{input_dir}."/".$study."/".$exp_file, ProtocolEncoding => 'ISO-8859-1');

      my $library_name = $doc->getElementsByTagName('LIBRARY_NAME')->item(0)->getFirstChild->getNodeValue()
	     if defined $doc->getElementsByTagName('LIBRARY_NAME')->item(0) && defined $doc->getElementsByTagName('LIBRARY_NAME')->item(0)->getFirstChild;
      my $library_strategy = $doc->getElementsByTagName('LIBRARY_STRATEGY')->item(0)->getFirstChild->getNodeValue();

      my $sample_ref = $doc->getElementsByTagName('SAMPLE_DESCRIPTOR')->item(0)->getAttributeNode('accession')->getNodeValue;

      my $sample_file = $sample_accession2file->{$sample_ref};
      unless (defined $sample_file) {
        print "  Skip sample accession: $sample_ref, no associated sample file exists\n";
        $settings->{log_fh}->print ("  Skip sample accession: $sample_ref, no associated sample file exists\n");
        next;
      }

      $doc = $parser->parsefile($settings->{input_dir}."/".$study."/".$sample_file, ProtocolEncoding => 'ISO-8859-1');

      my $sample_alias = $doc->getElementsByTagName('SAMPLE')->item(0)->getAttributeNode('alias')->getNodeValue;

      my $sample_attributes = $doc->getElementsByTagName('SAMPLE_ATTRIBUTE');
      my ($sample_id, $donor_id);
      for( my $j = 0; $j < $sample_attributes->getLength; $j++ ) {  # find all TAG/VALUE
        my $tag = $sample_attributes->item($j)->getElementsByTagName('TAG', 0)->item(0)->getFirstChild->getNodeValue();;
        next unless (lc($tag) eq 'sample id' || lc($tag) eq 'donor id');

        my $value = $sample_attributes->item($j)->getElementsByTagName('VALUE', 0)->item(0)->getFirstChild->getNodeValue();;

        $sample_id = $value if (lc($tag) eq 'sample id');
        $donor_id = $value if (lc($tag) eq 'donor id');

      }

      #print "Sample ID: $sample_id\tDonor ID: $donor_id\n";


      push @{$output}, [$dataset_accession, $title, $center_name, $sample_file, $sample_alias, $sample_id, $donor_id, $library_name, $library_strategy, $seq_file_type_name];
    }


    my ($uniq_datasets, @dataset_title, @submitter, $uniq_submitter, $uniq_samples, $uniq_strategies, $submitter_sample_id, $submitter_donor_id);

    foreach (@{$output}) {

      push @dataset_title, $_->[1] unless $uniq_datasets->{$_->[0]}++;
      push @submitter, $_->[2] unless $uniq_submitter->{$_->[2]}++;
      $uniq_samples->{$_->[3]}++;
      $uniq_strategies->{$_->[8]}++;
      $submitter_sample_id->{$_->[5]}++ if (length($_->[5]) > 0);
      $submitter_donor_id->{$_->[6]}++ if (length($_->[6]) > 0);
    }

    my $fh_summary = IO::File->new($settings->{output_dir} . "/" . $study_accession . ".sum", "w");
    $fh_summary->print("study_accession\tsubmitter\tnumber_of_datasets\tdataset_title\tnumber_of_samples\t#_of_ICGC_Sample_ID\t#_of_ICGC_Donor_ID\tlibrary_strategies\n");

    $fh_summary->print("$study_accession\t");
    $fh_summary->print(join(", ", @submitter)."\t");
    $fh_summary->print((keys(%{$uniq_datasets}) + 0)."\t");
    $fh_summary->print(join(", ", @dataset_title)."\t");
    $fh_summary->print((keys(%{$uniq_samples}) + 0)."\t");
    $fh_summary->print((keys(%{$submitter_sample_id}) + 0)."\t");
    $fh_summary->print((keys(%{$submitter_donor_id}) + 0)."\t");
    $fh_summary->print((join ",", sort keys(%{$uniq_strategies}))."\n");

    $fh_summary->close;


    my $fh_report = IO::File->new($settings->{output_dir} . "/" . $study_accession . ".out", "w");

    # print header
    $fh_report->print("DataSet accession\tDataSet title\tSubmitter\tSample XML file\tSample alias\tSubmitter's sample ID\tSubmitter's donor ID\tLibrary name\tLibrary strategy\tFileType:FileName\n") if (@{$output} > 0);

    $fh_report->print(join("\t", @{$_}) . "\n") foreach (@{$output});

    $fh_report->close;

  }

  if ($success) {
    print "Processing study: $study successfully completed.\n\n";
    $settings->{log_fh}->print ("[INFO] Processing study: $study successfully completed.\n");
  } else {
    print "Processing study: $study terminated, see stderr for details.\n\n";
    $settings->{log_fh}->print ("[ERROR] Processing study: $study terminated before completion.\n");
  }

  $settings->{log_fh}->close;

}


sub build_accession2file {
  my ($folder, $file_pattern, $xml_el, $xml_att) = @_;
  my $files = find_file($folder, $file_pattern);

  my $accession2file = {};

  foreach (@{ $files }) {
    my $doc = $parser->parsefile($folder.'/'.$_, ProtocolEncoding => 'ISO-8859-1');

    my $accession = $doc->getElementsByTagName($xml_el)->item(0)->getAttributeNode($xml_att)->getNodeValue;

    $accession2file->{$accession} = $_ if (defined $accession);
  }

  return $accession2file;
}

# this function validates the input and settings
sub verify_and_prepare {
   my $settings = shift;

   die "\nError: Re-annotation directory (". $settings->{annotation_dir} .") does not exist\n\n"
      if ($settings->{use_reannotation} && !(-d $settings->{annotation_dir})) ;


   unless (-d $settings->{input_dir}) { ## input dir does not exist

      die "\nError: Input directory (". $settings->{input_dir} .") does not exist\n\n";

   } elsif (!@{$settings->{study}}) {  ## no study specified in the command line, we add all sub dirs as studys

      opendir(DIR, $settings->{input_dir}) or die $!;

      while (my $proj = readdir(DIR)) {
         next unless (-d $settings->{input_dir}."/".$proj && $proj ne "." && $proj ne "..");
         push @{$settings->{study}}, $proj;
      }
      closedir(DIR);

      if (!@{$settings->{study}}) {
         die "\nError: no submission in the input directory: " . $settings->{input_dir} . "\n\n";
      }

   } else { # project key(s) specified in command line, now we test if they exist or not

      my @proj_exist = ();

      foreach (@{$settings->{study}}) {
         if (-d $settings->{input_dir}."/".$_) {
            push @proj_exist, $_;
         } else {
            warn "Warning: specified study: $_ does not exist in the input directory, skipping ...\n";
         }
      }

      if (@proj_exist) {
         $settings->{study} = \@proj_exist;
      } else {
         die "\nError: none of the specified project key(s) exists.\n\n";
      }

   }

   unless (-d $settings->{output_dir}) { ## output dir does not exist
      warn "Output directory: " . $settings->{output_dir} . " does not exist, trying to create it ...\n";

      eval { mkpath($settings->{output_dir}) };

      die "\nCouldn't create output directory: $@\n\n" if ($@);

      warn "Output dir created!\n";
   }

}

sub uniq {
   return keys %{{ map { $_ => 1 } @_ }};
}

sub find_file {
   my ($dir, $pattern) = @_;

   my $files = [];

   eval {
      opendir(DIR, $dir);

      while (my $f = readdir(DIR)) {
         push @{$files}, $f if ($f =~ /$pattern/);
      }

      closedir(DIR);
   };

   return $files;

}
