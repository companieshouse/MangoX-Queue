package MangoX::Queue::Job;

use Mojo::Base 'Mojo::EventEmitter';

has 'has_finished' => sub { 0 };

sub DESTROY {
    my ($self) = @_;
    
    $self->finished;
}

sub finished {
    my ($self) = @_;

    return if $self->has_finished;
    $self->has_finished(1);
    $self->emit_safe("finished");
}

1;

=encoding utf8

=head1 NAME

MangoX::Queue::Job - A job consumed from L<MangoX::Queue>

=head1 DESCRIPTION

L<MangoX::Queue::Job> is an object representing a job that has been consumed from L<MangoX::Queue>.
The object is just a document/job retrieved from the queue that is blessed, with an added desructor
method.

This class is used internally by L<MangoX::Queue>

=head1 SYNOPSIS

    use MangoX::Queue::Job;

    my $doc = {...};

    my $job = new MangoX::Queue::Job($doc);
    $job->queue($self);
    undef($job); # or let $job fall out of scope/refcount to 0

=head1 ATTRIBUTES

L<MangoX::Queue::Job> implements the following attributes:

=head2 queue

    $job->queue($queue);

Holds the L<MangoX::Queue> instance that L<MangoX::Queue::Job> belongs to. It must be set when the job is created.

=head1 METHODS

L<MangoX::Queue::Job> implements the following methods:

=head2 DESTROY

Called automatically when C<$job> goes out of scope, undef'd, or refcount goes to 0.

=head1 SEE ALSO

L<MangoX::Queue::Tutorial>, L<Mojolicious>, L<Mango>

=cut
