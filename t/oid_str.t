#!/usr/bin/env perl

use strict;
use warnings;
use v5.10;

use Mango;
use MangoX::Queue;

use Test::More;

my $mango = Mango->new($ENV{MANGO_URI} // 'mongodb://localhost:27017');
my $collection = $mango->db('test')->collection('mangox_queue_test');
eval { $collection->drop };
$collection->create;

my $queue = MangoX::Queue->new(collection => $collection);

$queue->no_binary_oid(1);
test_oid_str();

$queue->no_binary_oid(0);
test_oid_bin();

sub test_oid_str {
	enqueue $queue '82365';

	my $happened = 0;

	my $consumer_id;
	$consumer_id = consume $queue sub {
		my ($job) = @_;

        is ref($job->{_id}), '', 'ObjectID is plain string';

		$happened++;
		if($happened == 1) {
			is($job->{data}, '82365', 'Found job 82365 in non-blocking consume');
			Mojo::IOLoop->timer(1 => sub {
				enqueue $queue '29345';
			});
		} elsif ($happened == 2) {
			is($job->{data}, '29345', 'Found job 29345 in non-blocking consume');
			release $queue $consumer_id;
			Mojo::IOLoop->stop;
		} else {
			use Data::Dumper; print Dumper $job;
			fail('Queue consumed too many items');
		}
	};

	is($happened, 0, 'Non-blocking consume successful');

	Mojo::IOLoop->start unless Mojo::IOLoop->is_running;
}

sub test_oid_bin {
	enqueue $queue '82365';

	my $happened = 0;

	my $consumer_id;
	$consumer_id = consume $queue sub {
		my ($job) = @_;

        is ref($job->{_id}), 'Mango::BSON::ObjectID', 'ObjectID is blessed object';

		$happened++;
		if($happened == 1) {
			is($job->{data}, '82365', 'Found job 82365 in non-blocking consume');
			Mojo::IOLoop->timer(1 => sub {
				enqueue $queue '29345';
			});
		} elsif ($happened == 2) {
			is($job->{data}, '29345', 'Found job 29345 in non-blocking consume');
			release $queue $consumer_id;
			Mojo::IOLoop->stop;
		} else {
			use Data::Dumper; print Dumper $job;
			fail('Queue consumed too many items');
		}
	};

	is($happened, 0, 'Non-blocking consume successful');

	Mojo::IOLoop->start unless Mojo::IOLoop->is_running;
}


done_testing;
