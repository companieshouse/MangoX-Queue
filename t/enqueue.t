#!/usr/bin/env perl

use strict;
use warnings;

use Mango;
use MangoX::Queue;

use Test::More;

my $mango = Mango->new('mongodb://localhost:27017');
my $collection = $mango->db('test')->collection('mangox_queue_test');
$collection->remove;

my $queue = MangoX::Queue->new(collection => $collection);

BEGIN {
	use Log::Declare;
	#Log::Declare->startup_level('TRACE');
}

trace "Enqueing job";
enqueue $queue 'test';

my $job = fetch $queue;
trace "Got job: %s", d:$job;

isnt($job, undef, 'Got job from queue');
is($job->{priority}, 1, 'Priority is right');
is($job->{status}, 'Pending', 'Status is right');
is($job->{data}, 'test', 'Data is right');

trace "Enqueing another job";
enqueue $queue +{
	name => 'job_name',
};

$job = fetch $queue;
trace "Got job: [%s] %s", r:$job, d:$job;

isnt($job, undef, 'Got job from queue');
is($job->{priority}, 1, 'Priority is right');
is($job->{status}, 'Pending', 'Status is right');
is(ref($job->{data}), 'HASH', 'Ref is right');
is($job->{data}->{name}, 'job_name', 'Inner data is right');

# To be notified when the job completes
#enqueue $queue 'test' => sub {
#	# ...
#};

done_testing;