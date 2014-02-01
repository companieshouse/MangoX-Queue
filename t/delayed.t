#!/usr/bin/env perl

use strict;
use warnings;

use Mango;
use MangoX::Queue;

use Test::More;

my $mango = Mango->new($ENV{MANGO_URI} // 'mongodb://localhost:27017');
my $collection = $mango->db('test')->collection('mangox_queue_test');
eval { $collection->drop };
$collection->create;

my $queue = MangoX::Queue->new(collection => $collection);

test_delay();

sub test_delay {
    my $started = time;
    my $delay_until = $started + 2;
	enqueue $queue delay_until => $delay_until, '82365';

	my $happened = 0;

	my $consumer_id;
	$consumer_id = consume $queue sub {
		my ($job) = @_;

        release $queue $consumer_id;

        my $finished = time;

        ok($finished > ($started + 2), 'Job took over 2 seconds to be picked up');
        is($job->{data}, '82365', 'Found job 82365 in non-blocking consume');

        Mojo::IOLoop->stop;
	};

	is($happened, 0, 'Non-blocking consume successful');

	Mojo::IOLoop->start unless Mojo::IOLoop->is_running;
}

done_testing;
