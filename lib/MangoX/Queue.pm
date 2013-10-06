package MangoX::Queue;

use Mojo::Base -base;

use Carp 'croak';
use DateTime;
use Log::Declare;
use Mango::BSON ':bson';
use MangoX::Queue::Delay;

our $VERSION = '0.01';

# The Mango::Collection representing the queue
has 'collection';

# A MangoX::Queue::Delay
has 'delay' => sub { MangoX::Queue::Delay->new };

# How long to wait before assuming a job has failed
has 'timeout' => sub { $ENV{MANGOX_QUEUE_JOB_TIMEOUT} // 60 };

# How many times to retry a job before giving up
has 'retries' => sub { $ENV{MANGOX_QUEUE_JOB_RETRIES} // 5 };

sub new {
	my $self = shift->SUPER::new(@_);

	croak qq{No Mango::Collection provided to constructor} unless ref($self->collection) eq 'Mango::Collection';

	return $self;
}

sub get_options {
	my ($self) = @_;

	return {
		query => {
			status => 'Pending'
	#		'$or' => [{
	#			status => {
	#				'$in' => [ 'Pending' ]
	#			}
	#		},{
	#			status => {
	#				'$in' => [ 'Retrieved' ]
	#			},
	#			retrieved => {
	#				'$gt' => DateTime->now->subtract()
	#			}
	#		}]
		},
		update => {
			'$set' => {
				status => 'Retrieved',
				retrieved => DateTime->now,
			},
			'$inc' => {
				attempt => 1,
			}
		},
		sort => bson_doc( # Sort by priority, then in order of creation
			'priority' => 1,
			'created' => -1,
		),
		new => 0, # Get the original object (so we can see status etc)
	};
}

sub enqueue {
	my ($self, $job, $callback) = @_;

	my $db_job = {
		priority => 1,
		created => DateTime->now,
		data => $job,
		status => 'Pending',
	};

	my $id = $self->collection->insert($db_job);

	if($callback) {
		# TODO monitor state
	}

	return $id;
}

sub fetch {
	my ($self, $callback) = @_;

	trace "In fetch" [QUEUE];

	if($callback) {
		trace "Fetching in non-blocking mode" [QUEUE];
		return Mojo::IOLoop->timer(0 => sub { $self->_watch_nonblocking($callback, 1) });
	} else {
		trace "Fetching in blocking mode" [QUEUE];
		return $self->_watch_blocking(1);
	}
}

sub watch {
	my ($self, $callback) = @_;

	trace "In watch" [QUEUE];

	if($callback) {
		trace "Watching in non-blocking mode" [QUEUE];
		return Mojo::IOLoop->timer(0 => sub { $self->_watch_nonblocking($callback, 0) });
	} else {
		trace "Watching in blocking mode" [QUEUE];
		return $self->_watch_blocking(0);
	}
}

sub _watch_blocking {
	my ($self, $fetch) = @_;

	while(1) {
		my $doc = $self->collection->find_and_modify($self->get_options);
		trace "Job found by Mango: %s", ($doc ? 'Yes' : 'No') [QUEUE];

		if($doc) {
			return $doc;
		} else {
			last if $fetch;
			$self->delay->wait;
		}
	}
}

sub _watch_nonblocking {
	my ($self, $callback, $fetch) = @_;

	$self->collection->find_and_modify($self->get_options => sub {
		my ($cursor, $err, $doc) = @_;
		trace "Job found by Mango: %s", ($doc ? 'Yes' : 'No') [QUEUE];
		
		if($doc) {
			$self->delay->reset;
			$callback->($doc);
			return unless Mojo::IOLoop->is_running;
			return if $fetch;
			Mojo::IOLoop->timer(0 => sub { $self->_watch_nonblocking($callback) });
		} else {
			$self->delay->wait(sub {
				return unless Mojo::IOLoop->is_running;
				return if $fetch;
				Mojo::IOLoop->timer(0 => sub { $self->_watch_nonblocking($callback) });
			});
			return undef;
		}
	});
}

1;

=encoding utf8

=head1 NAME

MangoX::Queue - A MongoDB queue implementation using Mango

=head1 SYNOPSIS

	use Mango;
	use MangoX::Queue;

	my $mango = Mango->new("mongodb://localhost:27017");
	my $collection = $mango->db('my_db')->collection('my_queue');

	my $queue = MangoX::Queue->new(collection => $collection);

	# To add a basic job
	enqueue $queue 'some job name';
	$queue->enqueue('some job name');

	# To add a complex job
	enqueue $queue +{
		foo => 'bar'
	};
	$queue->enqueue({
		foo => 'bar'
	});

	# To fetch a job (blocking)
	my $job = fetch $queue;
	my $job = $queue->fetch;

	# To fetch a job (non-blocking)
	fetch $queue sub {
		my ($job) = @_;
		# ...
	};
	$queue->fetch(sub {
		my ($job) = @_;
		# ...
	});

	# To watch a queue (blocking)
	while (my $job = watch $queue) {
		# ...
	}
	while (my $job = $queue->watch) {
		# ...
	}

	# To watch a queue (non-blocking)
	watch $queue sub {
		my ($job) = @_;
		# ...
	};
	$queue->watch(sub{
		my ($job) = @_;
		# ...
	});

=head1 DESCRIPTION

L<MangoX::Queue> is a MongoDB backed queue implementation using L<Mango> to support
blocking and non-blocking queues.

L<MangoX::Queue> makes no attempt to handle the L<Mango> connection, database or
collection - pass in a collection to the constructor and L<MangoX::Queue> will
use it. The collection can be plain, capped or sharded.

=head1 ATTRIBUTES

L<MangoX::Queue> implements the following attributes.

=head2 delay

	my $delay = $queue->delay;
	$queue->delay(MangoX::Queue::Delay->new);

The L<MangoX::Queue::Delay> responsible for dynamically controlling the
delay between queue queries.

=head2 collection

    my $collection = $queue->collection;
    $queue->collection($mango->db('foo')->collection('bar'));

    my $queue = MangoX::Queue->new(collection => $collection);

The L<Mango::Collection> representing the MongoDB queue collection.

=head2 retries

	my $retries = $queue->retries;
	$queue->retries(5);

The number of times a job will be picked up from the queue before it is
marked as failed.

=head2 timeout

	my $timeout = $queue->timeout;
	$queue->timeout(10);

The time (in seconds) a job is allowed to stay in Retrieved state before
it is released back into Pending state. Defaults to 60 seconds.

=head1 METHODS

L<MangoX::Queue> implements the following methods.

=head2 enqueue

	enqueue $queue 'job name';
	enqueue $queue [ 'some', 'data' ];
	enqueue $queue +{ foo => 'bar' };

	$queue->enqueue('job name');
	$queue->enqueue([ 'some', 'data' ]);
	$queue->enqueue({ foo => 'bar' });

Add an item to the queue.

Currently uses priority 1 with a job status of 'Pending'.

=head2 fetch

	# In blocking mode
	my $job = fetch $queue;
	my $job = $queue->fetch;

	# In non-blocking mode
	fetch $queue sub {
		my ($job) = @_;
		# ...
	};
	$queue->fetch(sub {
		my ($job) = @_;
		# ...
	});

Fetch a single job from the queue, returning undef if no jobs are available.

Currently sets job status to 'Retrieved'.

=head2 get_options

	my $options = $queue->get_options;

Returns the L<Mango::Collection> options hash used by find_and_modify to
identify and update available queue items.

=head2 watch

	# In blocking mode
	while(my $job = watch $queue) {
		# ...
	}
	while(my $job = $queue->watch) {
		# ...
	}

	# In non-blocking mode
	watch $queue sub {
		my ($job) = @_;
		# ...
	};
	$queue->watch(sub {
		my ($job) = @_;
		# ...
	});

Watches the queue for jobs, sleeping between queue checks using L<MangoX::Queue::Delay>.

Currently sets job status to 'Retrieved'.

=head1 SEE ALSO

L<Mojolicious>, L<Mango>

=cut