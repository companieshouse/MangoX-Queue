package MangoX::Queue::Job;

use Mojo::Base -base;

has 'queue' => sub { die('queue not defined') };

sub DESTROY
{
    my $self = shift;

    $self->queue->job_count($self->queue->job_count - 1);

    return;
}

1;

=encoding utf8

=head1 NAME

MangoX::Queue::Job - A job consumed from L<MangoX::Queue>

=head1 DESCRIPTION

L<MangoX::Queue::Job> is an object representing a job that has been consumed from L<MangoX::Queue>.
The object is just a document/job retrieved from the queue that is blessed, with some added
methods (see L<METHODS>).

This class is used internally by L<MangoX::Queue>

=head1 SYNOPSIS

    use MangoX::Queue::Job;

    my $doc = {foo => 'bar', ...};

    my $job = MangoX::Queue::Job->new($doc)->on_finish(sub {
        $self->job_count($self->job_count - 1);
    }));

    $job->finish;

=head1 ATTRIBUTES

L<MangoX::Queue::Job> implements the following attributes:

=head2 on_finish

    $job->on_finish(sub{ ... });

The code called when L</finish> is called. Defaults to C<sub {}>, i.e. do nothing, but
L<MangoX::Queue> should set this to be CODE that decrements L<job_count|MangoX::Queue/job_count>

=head1 METHODS

L<MangoX::Queue::Job> implements the following methods:

=head2 finish

    $job->finish;

Calls the code previously passed to </on_finish>. This is called automatically when C<$job> goes
out of scope, gets set to something else, or gets DESTROYed.

=head1 SEE ALSO

L<MangoX::Queue::Tutorial>, L<Mojolicious>, L<Mango>

=cut
