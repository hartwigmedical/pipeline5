#!/usr/bin/env perl
use strict;
use warnings;

# This script performs two tasks:
# 1. change from #!/usr/bin/perl to #!/usr/bin/env perl
# 2. explicitly use python3 for famdb.py

if (@ARGV != 1) {
    die "Usage: $0 <repeatMaskerDir>\n";
}

my $repeatMaskerDir = $ARGV[0];

print "repeatMaskerDir is set to: $repeatMaskerDir\n";

# NOTE: following code is copied and modified from the configure script from repeatMasker

##
## Alter perl invocation headers
##
print " -- Setting perl interpreter...\n";
my @progFiles = (
                  "RepeatMasker",
                  "DateRepeats",
                  "ProcessRepeats",
                  "RepeatProteinMask",
                  "DupMasker",
                  "addRepBase.pl",
                  "util/calcDivergenceFromAlign.pl",
                  "util/createRepeatLandscape.pl",
                  "util/maskFile.pl",
                  "util/rmOutToGFF3.pl",
                  "util/buildRMLibFromEMBL.pl",
                  "util/rmToUCSCTables.pl"
);
my $perlLocEsc = "/usr/bin/env perl";
$perlLocEsc =~ s/\//\\\//g;

foreach my $file ( @progFiles ) {
  system(
         "perl -i -0pe \'s/^#\\!.*perl.*/#\\!$perlLocEsc/g;' $repeatMaskerDir/$file" );
}

# also set script to use python3 (is this really needed?)
system("perl -i -pe \'s/\\\$REPEATMASKER_DIR\\/famdb\\.py/python3 \\\$REPEATMASKER_DIR\\/famdb.py/g;' $repeatMaskerDir/RepeatMasker");
