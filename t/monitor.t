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

# Note - no easy/sensible way to test blocking monitor
# But we'll check it at least returns
my $id = enqueue $queue status => 'Complete', 'test';
monitor $queue $id, 'Complete';
ok(1, 'Blocking monitor returned');

# Single monitor watching a single status

$id = enqueue $queue 'test';

monitor $queue $id, 'Complete' => sub {
	ok(1, 'Job status is complete');
	Mojo::IOLoop->stop;
};

Mojo::IOLoop->timer(1 => sub {
	$collection->update({'_id' => $id}, { '$set' => {'status' => 'Complete'}});
});

Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

# Single monitor watching multiple statuses

$id = enqueue $queue 'test';
monitor $queue $id, ['Complete','Failed'] => sub {
	ok(1, 'Job status is complete or failed');
	$collection->update({'_id' => $id}, { '$set' => {'status' => 'Pending'}});
	monitor $queue $id, ['Complete','Failed'] => sub {
		ok(1, 'Job status is complete or failed');
		Mojo::IOLoop->stop;
	};
	Mojo::IOLoop->timer(1 => sub {
		$collection->update({'_id' => $id}, { '$set' => {'status' => 'Failed'}});
	});
};

Mojo::IOLoop->timer(1 => sub {
	$collection->update({'_id' => $id}, { '$set' => {'status' => 'Complete'}});
});

Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

# Separate complete/failed monitors

$id = enqueue $queue 'test';

monitor $queue $id, 'Complete' => sub {
	ok(1, 'Job status is complete');
	Mojo::IOLoop->timer(1 => sub {
		$collection->update({'_id' => $id}, { '$set' => {'status' => 'Failed'}});
	});
};
monitor $queue $id, 'Failed' => sub {
	ok(1, 'Job status is failed');
	Mojo::IOLoop->stop;
};

Mojo::IOLoop->timer(1 => sub {
	$collection->update({'_id' => $id}, { '$set' => {'status' => 'Complete'}});
});

Mojo::IOLoop->start unless Mojo::IOLoop->is_running;


done_testing;