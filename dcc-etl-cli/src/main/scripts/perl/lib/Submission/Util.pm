package Submission::Util;

use 5.01000;
use strict;
use warnings;
use JSON;
use File::Path;
use Data::Dumper;
use MongoDB;

require Exporter;

our @ISA = qw(Exporter);

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.

# This allows declaration	use Submission::Util ':all';
# If you do not need this, moving things directly into @EXPORT or @EXPORT_OK
# will save memory.
our %EXPORT_TAGS = ( 'all' => [ qw(
	
) ] );

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

our @EXPORT = qw(
    &rand_base &diff_base &easy_dict &uniq &verify_and_prepare &create_output_dir &find_file
    &column_name_to_index &generate_sequencing_strategy_data
    &parse_json_file
);

our $VERSION = '0.01';


# Preloaded methods go here.
sub easy_dict {
    my $dict_file = shift;

    open (IN, "< $dict_file") || die "Couldn't open the specified Dictionary file: $dict_file. Make sure the file exists and accessible.\n";

    my $dict_json_string = "";

    while(<IN>){
      chomp;
      $dict_json_string .= $_;
    }

    my $dict_json = from_json ($dict_json_string, { utf8  => 1 } );

    my $easy_dict = {};

    foreach (@{$dict_json->{files}}) {
        my $file_name = $_->{name};
        my $file_pattern = $_->{pattern};
        my $fields = {};
        my $field_list = [];
        my $controlled_fields = {};
        foreach (@{$_->{fields}}) {
            my $field = $_->{name};
            push @{$field_list}, $field;
 
            # check for controlled access field
            if ($_->{"controlled"} eq "true") {
              $controlled_fields->{$field} = 1;
            }

            my $required = 0;
            foreach (@{$_->{restrictions}}) {
               if ($_->{type} eq "required" && $_->{config}->{acceptMissingCode}) {

                  $required = 1;

               }elsif ($_->{type} eq "required") {

                  $required = 2;

               }
            }

            $fields->{$field} = $required;
        }
        $easy_dict->{$file_name}->{pattern} = $file_pattern;
        $easy_dict->{$file_name}->{fields} = $fields;
        $easy_dict->{$file_name}->{field_list} = $field_list;
        $easy_dict->{$file_name}->{controlled_fields} = $controlled_fields;
    }

    return $easy_dict;

}

sub parse_json_file {
    my $file = shift;
    return from_json ($file) if ($file =~ /^\s*\{.*\}\s*$/); # if json string specified, return it

    open (IN, "< $file") || die "Couldn't open specified JSON file: $file. Make sure it exists and accessible\n";

    local $/;
    my $json_string = <IN>;

    return from_json ($json_string);

}

sub rand_base {
   my @bases = ("T", "C", "G", "A");
   return $bases[int(rand(4))];
}

sub diff_base {
   my $base = shift;
   my $diff_base = $base;

   while ($base eq $diff_base) {
      $diff_base = &rand_base();
   }

   return $diff_base;
}

sub create_output_dir {
   my ($project_key, $settings) = @_;

   if (-d $settings->{output_dir} . "/" . $project_key ) {

      my $ans;

      if ($settings->{force_overwrite}) {

         $ans = "Y";

      } else {

         print "Output directory for project: $project_key exists already, do you want to delete it?\nType 'y or Y' to confirm deletion, otherwise, keep the existing one. Your choice:";

         $| = 1;
         $ans = <STDIN>;

      }

      if ($ans =~ /^y$/i) {
         eval {
            rmtree ($settings->{output_dir} . "/" . $project_key)
              || die "Couldn't remove the existing directory. Skipping this project: $project_key\n";
         };

         if ($@) {
            print $@;
            return 0;
         }

      } else {
         print "Skipping this project: $project_key\n";
         return 0;
      }
   }

   mkpath ($settings->{output_dir} . "/" . $project_key)
      || die "\nCould not create output directory: " . $settings->{output_dir} . "/" . $project_key . "\n\n";

   return 1;
}

# function to find files matching name pattern
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


# this funtion takes a file header as input, returns a Hash ref with keys being column_name, values being position of the column
sub column_name_to_index {
   my $header_line = shift;

   $header_line =~ s/\n//; # remove new line if any
   $header_line =~ s/\r//g; # remove the insane ^M

   my @column_names = split /\t/, $header_line;

   my $column_name_to_index = {};

   my $i = 0;
   foreach (@column_names) {
      $column_name_to_index->{$_} = $i;

      $i++;
   }

   return $column_name_to_index;
}


sub uniq {
    return keys %{{ map { $_ => 1 } @_ }};
}

sub generate_sequencing_strategy_data {
    my ($project_key, $sequencing_strategy, $mongodb_uri) = @_;

    my ($host, $db, $collection);
    if ($mongodb_uri =~ /^mongodb:\/\/(.+?)\/(.+)\.(.+)$/) {
       ($host, $db, $collection) = ($1, $2, $3);
    } else {
       warn "[ERROR] Specified mongodb_uri incorrect. Make sure host, port, db, collection are all set properly.\n";
       return 0;
    }

    my $mongo = MongoDB::MongoClient->new(host => "mongodb://$host", w => 1);

    my $seq_analysis = $mongo->get_database($db)->get_collection( $collection );

    my @data;

    foreach (keys %{$sequencing_strategy->{data}}) {

       my $legacy_sample_id = $_;
       my $_project_id = $project_key;
       foreach (keys(%{$sequencing_strategy->{data}->{$legacy_sample_id}})) {
             my $data = {
                           legacy_sample_id => $legacy_sample_id,
                           _project_id => $_project_id,
                        };
             $data->{library_strategy} = $_; 
             $data->{repository} = $sequencing_strategy->{data}->{$legacy_sample_id}->{$_}->[0];
             $data->{raw_data_accession} = $sequencing_strategy->{data}->{$legacy_sample_id}->{$_}->[1];
   
             push @data, $data;
       }
    }

    my @ids = ();

    if (@data > 0) {
       # drop existing docs for the current project
       $seq_analysis->remove({ '_project_id' => $project_key });

       @ids = $seq_analysis->batch_insert(\@data);
    } else {
       warn "[INFO] No sequencing analysis data for the current project to load into MongoDB.\n";
       return 1;
    }

    if (@ids > 0) {
       return 1;
    } else {
       warn "[ERROR] Something wrong while loading data into mongodb, please ensure mongodb is accessible.\n";
       return 0;
    }
}

# this function validates the input and settings
sub verify_and_prepare {
   my $settings = shift;

   die "\nError: Re-annotation directory (". $settings->{annotation_dir} .") does not exist\n\n"
      if ($settings->{use_reannotation} && !(-d $settings->{annotation_dir})) ;


   unless (-d $settings->{input_dir}) { ## input dir does not exist

      die "\nError: Input directory (". $settings->{input_dir} .") does not exist\n\n";

   } elsif (!@{$settings->{project_key}}) {  ## no project_key specified in the command line, we add all sub dirs as project_keys

      opendir(DIR, $settings->{input_dir}) or die $!;

      while (my $proj = readdir(DIR)) {
         next unless (-d $settings->{input_dir}."/".$proj && $proj ne "." && $proj ne "..");
         push @{$settings->{project_key}}, $proj;
      }
      closedir(DIR);

      if (!@{$settings->{project_key}}) {
         die "\nError: no submission in the input directory: " . $settings->{input_dir} . "\n\n";
      }

   } else { # project key(s) specified in command line, now we test if they exist or not

      my @proj_exist = ();

      foreach (@{$settings->{project_key}}) {
         if (-d $settings->{input_dir}."/".$_) {
            push @proj_exist, $_;
         } else {
            warn "Warning: specified project_key: $_ does not exist in the input directory, skipping ...\n";
         }
      }

      if (@proj_exist) {
         $settings->{project_key} = \@proj_exist;
      } else {
         die "\nError: none of the specified project key(s) exists.\n\n";
      }

   }

   unless (-d $settings->{output_dir}) { ## output dir does not exist
      warn "Output directory: " . $settings->{output_dir} . " does not exist, trying to create it ...\n";

      eval { mkpath($settings->{output_dir}) };
      eval { mkpath($settings->{output_dir}.'/reports') };

      die "\nCouldn't create output directory: $@\n\n" if ($@);

      warn "Output dir created!\n";
   } else {

      eval { mkpath($settings->{output_dir}.'/reports') } unless (-d $settings->{output_dir}.'/reports');

   }
}



# Autoload methods go after =cut, and are processed by the autosplit program.

1;
__END__
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

Submission::Util - Perl extension for blah blah blah

=head1 SYNOPSIS

  use Submission::Util;
  blah blah blah

=head1 DESCRIPTION

Stub documentation for Submission::Util, created by h2xs. It looks like the
author of the extension was negligent enough to leave the stub
unedited.

Blah blah blah.

=head2 EXPORT

None by default.



=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

Junjun Zhang, E<lt>junjun.zhang@oicr.on.ca<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2013 by Junjun Zhang

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.10.1 or,
at your option, any later version of Perl 5 you may have available.


=cut
