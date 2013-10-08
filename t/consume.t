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

test_nonblocking_consume();
test_blocking_consume();

sub test_nonblocking_consume {
	enqueue $queue 'test';

	my $happened = 0;

	consume $queue sub {
		my ($job) = @_;

		$happened++;
		ok(1, 'Found job ' . $happened . ' in non-blocking consume');

		if($happened == 2) {
			Mojo::IOLoop->stop;
			return;
		}

		Mojo::IOLoop->timer(1 => sub {
			enqueue $queue 'another test';
		});
	};

	is($happened, 0, 'Non-blocking consume successful');

	Mojo::IOLoop->start unless Mojo::IOLoop->is_running;
}

sub test_blocking_consume {
	enqueue $queue 'test';

	while(my $item = consume $queue) {
		ok(1, 'Found job in blocking consume');
		last;
	}
}

done_testing;