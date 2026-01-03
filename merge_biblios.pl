#!/usr/bin/perl

# merge_biblios.pl - Batch merge duplicate bibliographic records in Koha
# 
# For Koha 24.05+ which has Koha::Biblio->merge_with() method
#
# Input: Text file with one merge group per line
#        Format: master_biblionumber,child1,child2,child3,...
#        First biblionumber is kept (master), rest are merged into it and deleted
#        Delimiter is comma by default, configurable via --delimiter
#
# What gets merged:
#   - Items (holdings)
#   - Holds/Reserves
#   - Acquisition orders
#   - Serial subscriptions
#   - Course reserves
#   - ILL requests
#   - Recalls
#   - Tags
#
# Usage:
#   perl merge_biblios.pl --file duplicates.csv [--verbose] [--log logfile.txt]
#   perl merge_biblios.pl --file duplicates.csv --commit [--verbose] [--log logfile.txt]
#
#   Default is dry-run mode. Use --commit to actually perform the merge.
#
# Author: Indranil Das Gupta <indradg@l2c2.co.in> for L2C2 Technologies
# License: GPL v3
#
# Acknowledgment: Kyle M Hall, ByWater Solutions for the original Record Merger plugin
#                 https://github.com/bywatersolutions/dev-koha-plugin-record-merger

use Modern::Perl;
use Getopt::Long;
use Pod::Usage;
use Try::Tiny;
use Text::CSV;

# Koha modules
use C4::Context;
use C4::Log qw( logaction );
use Koha::Biblios;
use Koha::Patrons;

# Command line options
my $input_file;
my $commit     = 0;  # Default: dry-run (safe)
my $verbose    = 0;
my $log_file;
my $help       = 0;
my $delimiter  = ',';
my $framework;       # MARC framework code (default: use master's framework)
my $default_fw = 0;  # Force default framework (empty string)
my $user_id;         # Borrowernumber for action_logs attribution

GetOptions(
    'file|f=s'         => \$input_file,
    'commit|c'         => \$commit,
    'verbose|v'        => \$verbose,
    'log|l=s'          => \$log_file,
    'delimiter|d=s'    => \$delimiter,
    'framework=s'      => \$framework,
    'default-framework'=> \$default_fw,
    'user|u=i'         => \$user_id,
    'help|h'           => \$help,
) or pod2usage(2);

pod2usage(1) if $help;
pod2usage("Error: --file is required") unless $input_file;
pod2usage("Error: File '$input_file' not found") unless -f $input_file;
pod2usage("Error: Cannot use both --framework and --default-framework") if $framework && $default_fw;

# Setup logging
my $LOG_FH;
if ($log_file) {
    open($LOG_FH, '>', $log_file) or die "Cannot open log file '$log_file': $!";
}

# Setup userenv for action_logs attribution
my $user_info = "(system/CLI)";
if ($user_id) {
    my $patron = Koha::Patrons->find($user_id);
    if ($patron) {
        # Set up minimal userenv so logaction() records this user
        C4::Context->set_userenv(
            $patron->borrowernumber,
            $patron->userid,
            $patron->cardnumber,
            $patron->firstname,
            $patron->surname,
            $patron->branchcode,
            undef,  # branch name (not required)
            0,      # flags
            undef,  # emailaddress
            undef,  # shibboleth
        );
        $user_info = $patron->firstname . " " . $patron->surname . " (" . $patron->borrowernumber . ")";
    } else {
        die "Error: Borrowernumber $user_id not found in database.\n";
    }
}

sub log_msg {
    my ($level, $msg) = @_;
    my $timestamp = localtime();
    my $line = "[$timestamp] [$level] $msg\n";
    
    print $line if $verbose || $level eq 'ERROR' || $level eq 'WARN';
    print $LOG_FH $line if $LOG_FH;
}

sub log_info  { log_msg('INFO',  shift); }
sub log_warn  { log_msg('WARN',  shift); }
sub log_error { log_msg('ERROR', shift); }
sub log_debug { log_msg('DEBUG', shift) if $verbose; }

# Statistics
my %stats = (
    total_groups     => 0,
    successful       => 0,
    failed           => 0,
    skipped          => 0,
    total_merged     => 0,
    items_moved      => 0,
    holds_moved      => 0,
    orders_moved     => 0,
    subscriptions_moved => 0,
);

log_info("=" x 60);
log_info("Koha Batch Biblio Merger");
log_info("=" x 60);
log_info("Input file: $input_file");
log_info("Mode: " . ($commit ? "COMMIT (changes will be saved)" : "DRY-RUN (no changes will be made)"));
my $fw_display = $default_fw ? "(default framework forced)" 
               : $framework ? $framework 
               : "(use master record's framework)";
log_info("Framework: $fw_display");
log_info("User: $user_info");
log_info("=" x 60);

# Open and process input file
open(my $fh, '<:encoding(UTF-8)', $input_file) or die "Cannot open '$input_file': $!";

my $csv = Text::CSV->new({
    binary    => 1,
    sep_char  => $delimiter,
    auto_diag => 1,
});

my $line_num = 0;

while (my $row = $csv->getline($fh)) {
    $line_num++;
    $stats{total_groups}++;
    
    # Clean up biblionumbers (trim whitespace)
    my @biblionumbers = map { s/^\s+|\s+$//gr } grep { defined && /\S/ } @$row;
    
    # Skip empty lines
    unless (@biblionumbers >= 2) {
        log_warn("Line $line_num: Skipping - need at least 2 biblionumbers to merge");
        $stats{skipped}++;
        next;
    }
    
    my $master_biblio = shift @biblionumbers;
    my @children = @biblionumbers;
    
    log_info("-" x 40);
    log_info("Line $line_num: Processing merge group");
    log_info("  Master: $master_biblio");
    log_info("  Children to merge: " . join(', ', @children));
    
    # Validate master biblio exists
    my $biblio = Koha::Biblios->find($master_biblio);
    unless ($biblio) {
        log_error("  Master biblio $master_biblio does not exist! Skipping group.");
        $stats{failed}++;
        next;
    }
    
    log_debug("  Master title: " . ($biblio->title // 'N/A'));
    
    # Validate all children exist and check framework consistency
    my @valid_children;
    my $master_fw = $biblio->frameworkcode // '';
    for my $child_bn (@children) {
        my $child = Koha::Biblios->find($child_bn);
        if ($child) {
            push @valid_children, $child_bn;
            log_debug("  Child $child_bn exists: " . ($child->title // 'N/A'));
            
            # Warn if framework differs
            my $child_fw = $child->frameworkcode // '';
            if ($child_fw ne $master_fw) {
                log_warn("  Child $child_bn has different framework ('$child_fw') than master ('$master_fw')");
            }
            
            # Count items for statistics
            my $item_count = $child->items->count;
            $stats{items_moved} += $item_count if $commit;
            log_debug("    Items: $item_count");
        } else {
            log_warn("  Child biblio $child_bn does not exist! Skipping this child.");
        }
    }
    
    unless (@valid_children) {
        log_warn("  No valid children to merge. Skipping group.");
        $stats{skipped}++;
        next;
    }
    
    # Determine framework to use
    my $use_framework;
    if ($default_fw) {
        $use_framework = '';  # Force default framework
    } elsif (defined $framework) {
        $use_framework = $framework;  # Use specified framework
    } else {
        $use_framework = $biblio->frameworkcode // '';  # Use master's framework
    }
    log_debug("  Using framework: '" . ($use_framework || 'default') . "'");
    
    # Perform the merge
    if ($commit) {
        try {
            # Use Koha::Biblio->merge_with() method (available in Koha 24.05+)
            # This handles: items, holds, orders, subscriptions, and deletes children
            my $result = $biblio->merge_with(\@valid_children, {
                frameworkcode => $use_framework,
            });
            
            if ($result) {
                log_info("  SUCCESS: Merged " . scalar(@valid_children) . " biblios into $master_biblio");
                $stats{successful}++;
                $stats{total_merged} += scalar(@valid_children);
                
                # Log what was merged if result contains details
                if (ref($result) eq 'HASH') {
                    log_debug("  Merge details: " . join(', ', map { "$_: $result->{$_}" } keys %$result));
                }
            } else {
                log_error("  FAILED: merge_with() returned false for master $master_biblio");
                $stats{failed}++;
            }
        } catch {
            log_error("  FAILED: Error during merge - $_");
            $stats{failed}++;
        };
    } else {
        log_info("  [DRY-RUN] Would merge " . scalar(@valid_children) . " biblios into $master_biblio (framework: " . ($use_framework || 'default') . ")");
        $stats{successful}++;
        $stats{total_merged} += scalar(@valid_children);
    }
}

close($fh);

# Print summary
log_info("=" x 60);
log_info("SUMMARY");
log_info("=" x 60);
log_info("Total merge groups processed: $stats{total_groups}");
log_info("Successful merges: $stats{successful}");
log_info("Failed merges: $stats{failed}");
log_info("Skipped (invalid): $stats{skipped}");
log_info("Total biblios merged (deleted): $stats{total_merged}");
log_info("Estimated items moved: $stats{items_moved}") if $stats{items_moved};
log_info("=" x 60);

if (!$commit) {
    log_info("This was a DRY-RUN. No changes were made to the database.");
    log_info("Use --commit flag to perform actual merge.");
}

close($LOG_FH) if $LOG_FH;

exit($stats{failed} > 0 ? 1 : 0);

__END__

=head1 NAME

merge_biblios.pl - Batch merge duplicate bibliographic records in Koha

=head1 SYNOPSIS

merge_biblios.pl --file <input.csv> [options]

 Options:
   --file, -f          Input file with merge groups (required)
   --commit, -c        Actually perform the merge (default: dry-run)
   --user, -u          Borrowernumber for action_logs attribution
   --framework         MARC framework code to use for merged records
   --default-framework Force default framework (even if master has another)
   --verbose, -v       Show detailed progress
   --log, -l           Write log to file
   --delimiter, -d     Field delimiter (default: comma)
   --help, -h          Show this help

=head1 DESCRIPTION

This script performs batch merging of duplicate bibliographic records.

The input file should contain one merge group per line, with biblionumbers
separated by a delimiter (comma by default, configurable via --delimiter).
The FIRST biblionumber in each line is the MASTER record (kept), and all
subsequent biblionumbers are CHILDREN that will be merged into the master
and then deleted.

By default, the merged record uses the master record's MARC framework.
Use --framework to specify a particular framework for all merges, or
use --default-framework to force the default framework (empty string)
even if the master record uses a different framework.

Example input file:

    75,801,802,803,804
    105,1494,1495,1496
    45,900,1591,1592

This would:
- Merge biblios 801,802,803,804 into 75 (then delete 801-804)
- Merge biblios 1494,1495,1496 into 105 (then delete 1494-1496)
- Merge biblios 900,1591,1592 into 45 (then delete 900,1591,1592)

=head1 WHAT GETS MERGED

The Koha::Biblio->merge_with() method handles:

- Items (holdings) - moved to master biblio
- Holds/Reserves - transferred and reordered
- Acquisition orders - updated to point to master
- Serial subscriptions - moved to master
- Course reserves - updated
- ILL requests - updated
- Recalls - transferred
- Tags - consolidated

=head1 ACTION LOGS

If CataloguingLog system preference is enabled, actions are logged to action_logs.
Use --user to attribute actions to a specific staff member; otherwise actions are
logged as system/CLI operations with no user attribution.

=head1 REQUIREMENTS

- Koha 24.05 or later (uses Koha::Biblio->merge_with() method)
- Database access configured via KOHA_CONF

=head1 EXAMPLES

Preview what would happen (default behavior):

    perl merge_biblios.pl -f duplicates.csv --verbose

Perform actual merge with logging:

    perl merge_biblios.pl -f duplicates.csv --commit --log merge_log.txt --verbose

Attribute actions to a specific staff user (borrowernumber 1) in action_logs:

    perl merge_biblios.pl -f duplicates.csv --commit --user 1 --verbose

Force a specific MARC framework for all merged records:

    perl merge_biblios.pl -f duplicates.csv --commit --framework FA

Force the default framework (even if master records use a different one):

    perl merge_biblios.pl -f duplicates.csv --commit --default-framework

Process a semicolon-delimited file:

    perl merge_biblios.pl -f duplicates.txt --delimiter ';' --commit

=head1 AUTHOR

Indranil Das Gupta <indradg@l2c2.co.in> for L2C2 Technologies

=head1 ACKNOWLEDGMENT

Kyle M Hall, ByWater Solutions for the original Record Merger plugin
L<https://github.com/bywatersolutions/dev-koha-plugin-record-merger>

=cut
