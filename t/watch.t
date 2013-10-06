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

test_nonblocking_watch();
test_blocking_watch();

sub test_nonblocking_watch {
	trace "Enqueing job";
	enqueue $queue 'test';

	my $happened = 0;

	watch $queue sub {
		my ($job) = @_;

		$happened++;
		ok(1, 'Found job ' . $happened . ' in non-blocking watch');

		if($happened == 2) {
			trace "Happened is 2";
			Mojo::IOLoop->stop;
			return;
		}

		Mojo::IOLoop->timer(1 => sub {
			trace "Enqueing another job";
			enqueue $queue 'another test';
		});
	};

	is($happened, 0, 'Non-blocking watch successful');

	trace "Starting IOLoop";
	Mojo::IOLoop->start unless Mojo::IOLoop->is_running;
}

sub test_blocking_watch {
	trace "Enqueing job";
	enqueue $queue 'test';

	while(my $item = watch $queue) {
		trace "Got job in blocking mode: %s", d:$item;
		ok(1, 'Found job in blocking watch');
		last;
	}
}

done_testing;