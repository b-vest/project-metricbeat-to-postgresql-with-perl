#!/usr/bin/perl

#######################################################
#                  hybrid-poller.pl
# This script will poll a Redis database for metricbeat
# data and convert it into a hybrid SQL table format.
# This relies on the metricbeat fields.asciidoc file
# that is available from the metricbeat github repo
#
#
# This program loops deep into JSON files to create the
# SQL inserts. At every level a new hash reference
# will be created so we do not end up with a long
# hashkey chainin.
########################################################

use Redis;
use DBI;
use Data::Dumper;
use JSON::XS;
use Getopt::Long;
use Scalar::Util qw(looks_like_number);


use strict;

#check if script is running
my @isRunning = qx(ps auxwwww | grep $0 | grep -v grep | grep -v $$);
if ( scalar(@isRunning) > 0 ) {
    print "Script is running. Exiting\n";
    exit;
}



GetOptions(
    'help'               => \my $help,
    'dbname=s'           => \my $dbname,
    'dbhost=s'           => \my $dbhost,
    'dbport=i'           => \my $dbport,
    'dbuser=s'           => \my $dbuser,
    'dbpass=s'           => \my $dbpass,
    'batchsize=i'        => \my $batchsize,
    'rkey=s'             => \my $rediskey,
    'debug=i'            => \my $debug,
    'fields_file=s'      => \my $fields_file,
    'build_fields_table' => \my $build_fields_table,
    'live'               => \my $live
) or die "Invalid options passed to $0\n";

$dbport    = 5432         if !$dbport;
$rediskey  = 'metricbeat' if !$rediskey;
$debug = 0 if !$debug;
$dbhost = "localhost" if !$dbhost;
if ( !$dbname || !$dbuser || !$dbpass || $help ) {
    print "Missing command line settings.\n";
    printHelp();
    exit;
}

if ($debug) {
    print "Using Values\n";
    print "--dbname $dbname --dbhost $dbhost --dbport $dbport --dbuser $dbuser --dbpass $dbpass --rkey $rediskey\n";
}

## Defaults to $ENV{REDIS_SERVER} or 127.0.0.1:6379
my $redisClient = Redis->new;

##Connect to postgres
my $dbh = DBI->connect( "dbi:Pg:dbname=$dbname;host=$dbhost;port=$dbport", $dbuser, $dbpass, { AutoCommit => 0, RaiseError => 1 } ) or die $DBI::errstr;

if ($build_fields_table) {
	buildFieldsTable();
	exit;
}

#Load hash reference of field names with all associated fields
#We do this once here so we do not have to constantly
#call the database looking for the field_id
#This is generally a static data set that is only updated
#if you upgrade your version of metricbeat
#working version 8.4.2
my $fieldNamesSql = "SELECT * from field_names";
print $fieldNamesSql. "\n" if $debug;
my $fieldNameRef = $dbh->selectall_hashref( $fieldNamesSql, "field_name" );

my $insertCounter = 0;
my @insertArray;


while (1) {
    my $metric = $redisClient->lpop($rediskey);

    unless ($metric) {
        print "No metrics. Sleeping\n" if $debug;
        sleep(2);
        next;
    }

    chomp($metric);
    my $metricHash = decode_json($metric);
    my $timestamp = $metricHash->{'@timestamp'};
    my $hostname = $metricHash->{host}->{name};
    my $eventModule = $metricHash->{event}->{module};
    my $metricSet = $metricHash->{metricset}->{name};
    my $deviceName = $eventModule;
    my $theseMetrics = $metricHash->{$eventModule}->{$metricSet};

    print "$timestamp -> $hostname -> $eventModule -> $metricSet\n" if $debug >= 2;
    print "$theseMetrics\n" if $debug >= 2;

    if ($theseMetrics->{name}) {
        print "This is a Device Name: $theseMetrics->{name}\n" if $debug >= 2;
        $deviceName = $theseMetrics->{name};
    }

    process_metrics($theseMetrics, "", $eventModule, $metricSet, $timestamp, $hostname, $deviceName);
}


sub process_metrics {
    my ($hash_ref, $field_prefix, $eventModule, $metricSet, $timestamp, $hostname, $deviceName) = @_;
    my $field_name;

    foreach my $key (keys %{$hash_ref}) {
        my $value = $hash_ref->{$key};
        if (ref($value) eq "HASH") {
            process_metrics($value, $field_prefix . ".$key", $eventModule, $metricSet, $timestamp, $hostname, $deviceName);
        } else {
            $field_name = $eventModule . "." . $metricSet . $field_prefix . ".$key";
            my $fieldCheckReturn = checkFieldNameExsts($field_name);
            my $field_id   = $fieldNameRef->{$field_name}->{field_id};
            push(@insertArray,[$timestamp, $field_id, $hostname, $deviceName, $value, $field_name]);
            ++$insertCounter;
            checkBatchSize(@insertArray);
        }
    }
}


sub checkBatchSize{
  my @thisArray = @_;
  if(scalar(@thisArray >= $batchsize)){
      #print scalar(@thisArray)."\n";
        my $insertReturn = processInsertArray(@thisArray);
  }
}

sub processInsertArray{
  my @thisArray = @_;
      my $insertMetric = "INSERT INTO metric_values (timestamp,field_id,hostname,device,value) VALUES(?,?,?,?,?)";
    my $sth = $dbh->prepare($insertMetric);
  foreach my $metric(@thisArray){
    #In this version we are going to skip text and empty values. Maybe in the future log these and or add support
    #print Dumper($metric);

    if(looks_like_number($$metric[4])){
      $sth->execute($$metric[0],$$metric[1],$$metric[2],$$metric[3],$$metric[4]);
    }
  }
  $dbh->commit();

  @insertArray = ();

}




sub checkFieldNameExsts{
	my $field_name = shift;
	#Some field_names are wildcarded in the fields.asciidoc. We will of course have no match for these.
	#Here we will verify that the field name exists, create it if it does not and return the created value
	if(!$fieldNameRef->{$field_name}){
		#print "Field Name Does Not Exist. Creating: ".$field_name."\n";
    my $insertQuery = "INSERT INTO field_names (field_name) VALUES('" . $field_name . "')";
    my $newFieldReturn = $dbh->do($insertQuery);
		my $fieldNamesSql = "SELECT * from field_names";
		print "Reload the Hash: ".$fieldNamesSql. "\n" if $debug;
		$fieldNameRef = $dbh->selectall_hashref( $fieldNamesSql, "field_name" );

$dbh->commit();

	}
}

sub buildFieldsTable {
    print "build_fields_table\n";

    if (!$fields_file) {
        print "Missing --fields_file\n";
        printHelp();
        exit;
    }

    open(my $fh, '<', $fields_file) or die "Failed to open file: $!";

    my $insertedCount = 0;
    my $updatedCount  = 0;

    while (my $line = <$fh>) {
        chomp($line);

        if ($line =~ /::/) {
            my ($field_name) = $line =~ /`\K[^`]+/;
            chomp(my $description = <$fh>);
            $description =~ s/'//g;

            my $insertQuery = "
                INSERT INTO field_names (field_name, description)
                VALUES ('$field_name', '$description')
                ON DUPLICATE KEY UPDATE description = '$description'
            ";

            my $sth = $dbh->prepare($insertQuery);
            $sth->execute();

            if ($sth->rows == 1) {
                ++$insertedCount;
            } elsif ($sth->rows == 2) {
                ++$updatedCount;
            }
        }
    }

    $dbh->commit();
    print "Finished processing fields_talbe. Inserted: $insertedCount Updated: $updatedCount\n";

    exit;
}


sub printHelp{
    print <<EOF;
metircbeat-redis-sql.pl HELP
  --debug 1 = basic debug, insert and sleep >1 evertying in the script that shows a print command, so, everything
	--dbname Name of the Postgresql database that will be used.
	--dbhost Hostname or ip address of the Postgresql server.
	--dbport Port of the Postgresql server (defaults to 5432)
	--dbuser Postgres user that has write permission to --dbname
	--dbpass Password for Postgres user stated above
	--rkey	 Redis server key where the metricbeat data is read from (defaults to metricbeat)
	--fields_file The file thiat build_fields_table will use to populate the field_names table.
                currently this is only compatible with the fields.asciidoc file available in the metricbeat source.
	--build_fields_table Read the fields.yml file at --fields_file and buile the SQL relational table at field_names
EOF
    exit;
}
