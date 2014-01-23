package MangoX::Queue::Job;

use Mojo::Base -base;

use Carp;

has 'on_finish' => sub {};

sub finish
{
    shift->on_finish->(@_);
}

*DESTROY = \&finish;

1;
