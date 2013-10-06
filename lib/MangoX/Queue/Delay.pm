package MangoX::Queue::Delay;

use Mojo::Base -base;

use Log::Declare;

has start     => sub { $ENV{MANGOX_QUEUE_DELAY_START}     // 0.1  };
has current   => sub { $ENV{MANGOX_QUEUE_DELAY_START}     // 0.1  };
has increment => sub { $ENV{MANGOX_QUEUE_DELAY_INCREMENT} // 0.1  };
has maximum   => sub { $ENV{MANGOX_QUEUE_DELAY_MAXIMUM}   // 10   };

sub reset {
	my ($self) = @_;
	
	trace "Reset delay to %s", $self->start [DELAY];

	$self->current($self->start);
}

sub wait {
	my ($self, $callback) = @_;

	my $delay = $self->current;
	trace "Current delay is %s", $delay [DELAY];

	my $incremented = $delay + $self->increment;
	trace "New delay is %s", $incremented [DELAY];

	if($incremented > $self->maximum) {
		trace "Limiting delay to maximum %s", $self->maximum [DELAY];
		$incremented = $self->maximum;
	}

	$self->current($incremented);

	if($callback) {
		trace "Non-blocking delay for %s", $delay [DELAY];
		Mojo::IOLoop->timer($delay => $callback);
	} else {
		trace "Sleeping for %s", $delay [DELAY];
		sleep $delay;
	}

	return $delay;
}

1;