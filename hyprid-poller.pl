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

if ( !$dbname || !$dbhost || !$dbuser || !$dbpass || $help ) {
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

do {
START:
    my $redisCounter = 0;
    my $metric  = $redisClient->lpop( $rediskey);

    if ( !$metric ) {

        #if no metrics sleep for 2 seconds and try again.
        print "No metrics. Sleeping\n" if $debug;
        sleep(1);
        goto START;
    }

        #Loop through the metric array. This is an array of JSON text
        #remove new line characters from end of string
        chomp($metric);

        #print $metric."\n";
        #Convert JSON string into perl Hash object
        my $metricHash = decode_json($metric);

        #Assign a few variables for needed infomration at the root of the Hash
        my $timestamp   = $metricHash->{'@timestamp'};
        my $hostname    = $metricHash->{host}->{name};
        my $eventModule = $metricHash->{event}->{module};
        my $metricSet   = $metricHash->{metricset}->{name};
        my $deviceName  = $eventModule;

        #This creates the hash that contains the values that we want to ingest
        #we create the $theseMetrics variable to hold the metrics hash so the
        #hash keychains to not get long and confusing
        my $theseMetrics = $metricHash->{$eventModule}->{$metricSet};
        #If $theseMetrics is empty skip it. Maybe add logging in the future for things that are skipped
        #for this project dropping it is fine.
        print $timestamp. " ->" . $hostname . " -> " . $eventModule . " -> " . $metricSet . "\n" if $debug >= 2;
        print $theseMetrics. "\n"                                                                if $debug >= 2;

        #At this point we have to start looping through the metrics to get
        #the names and values of the metrics.
        #These can be up to four layers deep depending on the type of
        #metric
        #first level loop
        if ( $theseMetrics->{name} ) {
            print "This is a Device Name: " . $theseMetrics->{name} . "\n" if $debug >= 2;
            $deviceName = $theseMetrics->{name};
        }
        foreach my $level_1 ( keys %{$theseMetrics} ) {
            #Create New Variable for this level
            my $level_1Hash = $theseMetrics->{$level_1};
            #Check to see if this is a key with a value or another hash to loop
            if ( ref($level_1Hash) eq "HASH" ) {
                #Here we will loop the variable that we created above which is actually $theseMetrics->{$level_1}
                foreach my $level_2 ( keys %{$level_1Hash} ) {
                    #second Level Loop
                    my $level_2Hash = $level_1Hash->{$level_2};
                    if ( ref($level_2Hash) eq "HASH" ) {
                        #third level loop
                        foreach my $level_3 ( keys %{$level_2Hash} ) {
                            my $level_3Hash = $level_2Hash->{$level_3};
                            if ( ref($level_3Hash) eq "HASH" ) {
                                #fourth level loop
                                foreach my $level_4 ( keys %{$level_3Hash} ) {
                                    my $level_4Hash = $level_3Hash->{$level_4};
                                    if ( ref($level_4Hash) eq "HASH" ) {
                                        #No hashes are above 4 deep in this data set
                                        #This is a place holder for logic flow
                                    }
                                    else {
                                        my $field_name = $eventModule . "." . $metricSet . "." . $level_1 . "." . $level_2 . "." . $level_3 . "." . $level_4;
                                        my $fieldCheckReturn = checkFieldNameExsts($field_name);
                                        my $field_id   = $fieldNameRef->{$field_name}->{field_id};
                                        push(@insertArray,[$timestamp, $field_id, $hostname, $deviceName, $level_4Hash, $field_name]);
                                        ++$insertCounter;
                                        checkBatchSize(@insertArray);
                                    }
                                }
                            }
                            else {
                                my $field_name = $eventModule . "." . $metricSet . "." . $level_1 . "." . $level_2 . "." . $level_3;
                                my $fieldCheckReturn = checkFieldNameExsts($field_name);
                                my $field_id   = $fieldNameRef->{$field_name}->{field_id};
                                push(@insertArray, [$timestamp, $field_id, $hostname, $deviceName, $level_3Hash, $field_name]);
                                ++$insertCounter;
                                checkBatchSize(@insertArray);
                            }
                        }
                    }
                    else {
                        my $field_name = $eventModule . "." . $metricSet . "." . $level_1 . "." . $level_2;
                        my $fieldCheckReturn = checkFieldNameExsts($field_name);
                        my $field_id   = $fieldNameRef->{$field_name}->{field_id};
                        push(@insertArray,[$timestamp, $field_id, $hostname, $deviceName, $level_2Hash, $field_name]);
                        ++$insertCounter;
                        checkBatchSize(@insertArray);
                    }
                }    #end $level_1Hash Loop
            }
            else {
                #This is jsut a place holder for logic flow
                #We do no inserts here because this can only contain the
                #name string which we assigned at the top of
                #this loop
            }
        }# end $theseMetricsHash Loop

} while 1;


sub checkBatchSize{
  my @thisArray = @_;
  if(scalar(@thisArray >= $batchsize)){
      #print scalar(@thisArray)."\n";
    my $insertReturn = processInsertArray(@thisArray);
  }
}

sub processInsertArray{
  my @thisArray = @_;
      my $insertMetric = "INSERT INTO metric_values_hypertable (timestamp,field_id,hostname,device,value) VALUES(?,?,?,?,?)";
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
		print "Field Name Does Not Exist. Creating: ".$field_name."\n";
    my $insertQuery = "INSERT INTO field_names (field_name) VALUES('" . $field_name . "')";
    my $newFieldReturn = $dbh->do($insertQuery);
		my $fieldNamesSql = "SELECT * from field_names";
		print "Reload the Hash: ".$fieldNamesSql. "\n" if $debug;
		$fieldNameRef = $dbh->selectall_hashref( $fieldNamesSql, "field_name" );

$dbh->commit();

	}
}

sub buildFieldsTable{
	    print "build_fields_table\n";
    if ( !$fields_file ) {
        print "Missing --fields_file\n";
        printHelp();
        exit;
    }
    my @fieldsLines = qx(cat $fields_file);
    my $fieldsLines;
    my $field_name;
    my $insertQuery;
    my $lineCounter   = 0;
    my $insertedCount = 0;
    my $updatedCount  = 0;
    my @insertArray;

    foreach my $line (@fieldsLines) {
        chomp($line);
        if ( $line =~ /::/ ) {
            chomp( $fieldsLines[ $lineCounter + 3 ] );
            $fieldsLines[ $lineCounter + 3 ] =~ s/'//g;

            #Check and ingest field_name if it does not exist
            if ( $insertQuery =~ "INSERT" ) {
                print "Do The insert\n";
                $insertQuery = "";
            }

            #print "Split Field Name\n";
            ( undef, $field_name, undef ) = split( /\`/, $line );

            #print $field_name."\n";
            #check if this row already exists in the database
            my $checkSql       = "SELECT * from field_names where field_name = '" . $field_name . "'";
            my $fieldNameCheck = $dbh->selectall_hashref( $checkSql, "field_name" );
            $dbh->commit();

            if ( $fieldNameCheck->{$field_name}->{field_id} ) {
                my $updateSql = "UPDATE field_names SET description = '" . $fieldsLines[ $lineCounter + 3 ] . "' WHERE field_name = '" . $field_name . "'";
                $dbh->do($updateSql);
                ++$updatedCount;
            }
            else {
                my $insertQuery = "INSERT INTO field_names (field_name, description) VALUES('" . $field_name . "','" . $fieldsLines[ $lineCounter + 3 ] . "')";
                $dbh->do($insertQuery);

                ++$insertedCount;
            }
        }

        #print "Field Name: ".$field_name." ".$line ."\n";
        ++$lineCounter;
    }

    $dbh->commit();
    print "Finished processing fields_talbe. Inserted: " . $insertedCount . " Updated: " . $updatedCount . " \n";

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
