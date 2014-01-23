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

test_nonblocking_consume();
test_blocking_consume();
test_custom_consume();
test_job_max_reached();

sub test_nonblocking_consume {
	enqueue $queue '82365';

	my $happened = 0;

	my $consumer_id;
	$consumer_id = consume $queue sub {
		my ($job) = @_;

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

sub test_blocking_consume {
	enqueue $queue 'test';

	while(my $item = consume $queue) {
		ok(1, 'Found job in blocking consume');
		last;
	}
}

sub test_custom_consume {
	$collection->remove;

	my $id = enqueue $queue 'custom consume test';

	my $happened = 0;

	my $consumer_id;
	$consumer_id = consume $queue status => 'Failed', sub {
		my ($job) = @_;

		isnt($job, undef, 'Found failed job in non-blocking custom consume');

		release $queue $consumer_id;
		Mojo::IOLoop->stop;
		return;
	};

	is($happened, 0, 'Non-blocking consume successful');

	Mojo::IOLoop->timer(1 => sub {
		my $job = get $queue $id;
		$job->{status} = 'Failed';
		update $queue $job;
	});

	Mojo::IOLoop->start unless Mojo::IOLoop->is_running;
}

sub test_job_max_reached {
	my $queue_job_max_backup = $queue->job_max;
	my $done = {};
	my $consumer_id;

	$queue->job_max(2);
	$queue->enqueue($_) for (1..3);

	$consumer_id = consume $queue sub {
		my ($job) = @_;

		$done->{"job$job->{data}"} = 1;
		say("*** got job$job->{data}");
		#$job->finish;
	};

	$queue->once(job_max_reached => sub {
		say("*** got job3");
		$done->{job3} = 1;
	});

	Mojo::IOLoop->timer(0 => sub { _wait_test_job_max_reached($consumer_id, $done); });

	Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

	ok($done->{job1}, 'Found job1 in non-blocking consume');
	ok($done->{job2}, 'Found job2 in non-blocking consume');
	ok($done->{job3}, 'Maximum number of jobs reached while trying to consume job3');

	$queue->job_max($queue_job_max_backup);
}

sub _wait_test_job_max_reached {
	my ($consumer_id, $done) = @_;
	#say(Dumper($done));

	if (keys(%$done) >= 3) {
		$queue->release($consumer_id);
		Mojo::IOLoop->stop;
	}
	else {
		Mojo::IOLoop->timer(0 => sub { _wait_test_job_max_reached($consumer_id, $done); });
	}
}

done_testing;
